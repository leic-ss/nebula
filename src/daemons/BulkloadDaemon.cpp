/* Copyright (c) 2025. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <errno.h>
#include <folly/ssl/Init.h>
#include <signal.h>
#include <string.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/NebulaKeyUtils.h"
#include "daemons/SetupLogging.h"
#include "version/Version.h"

#include "codec/RowWriterV2.h"

#include "clients/meta/MetaClient.h"
#include "parser/MutateSentences.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"

#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"

#include <vector>

using nebula::ProcessUtils;
using nebula::Status;
using nebula::StatusOr;
using nebula::fs::FileUtils;
using nebula::network::NetworkUtils;

DEFINE_bool(daemonize, true, "Whether run as a daemon process");
DEFINE_string(pid_file, "pids/nebula-bulkload.pid", "File to hold the process id");
DEFINE_string(datafile, "/tmp/datafile", "Specify the data file");
DEFINE_string(meta_server_addrs, "127.0.0.1:9669", "Specify the meta server addrs");

static void printHelp(const char *prog) {
    fprintf(stderr, "%s --datafile <data_file>\n", prog);
}

int main(int argc, char *argv[]) {
    google::SetVersionString(nebula::versionString());
    google::SetUsageMessage("Usage: " + std::string(argv[0]) + " [options]");
    google::SetStderrLogging(google::INFO);

    if (argc == 1) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 2) {
        if (::strcmp(argv[1], "-h") == 0) {
            printHelp(argv[0]);
            return EXIT_SUCCESS;
        }
    }

    folly::init(&argc, &argv, true);

    if (FLAGS_datafile.empty()) {
        printHelp(argv[0]);
        return EXIT_FAILURE;
    }

    // Setup logging
    auto status = setupLogging(argv[0]);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }

    // Detect if the server has already been started
    auto pidPath = FLAGS_pid_file;
    status = ProcessUtils::isPidAvailable(pidPath);
    if (!status.ok()) {
        LOG(ERROR) << status;
        return EXIT_FAILURE;
    }

    if (FLAGS_daemonize) {
        status = ProcessUtils::daemonize(pidPath);
        if (!status.ok()) {
            LOG(ERROR) << status;
            return EXIT_FAILURE;
        }
    } else {
        // Write the current pid into the pid file
        status = ProcessUtils::makePidFile(pidPath);
        if (!status.ok()) {
            LOG(ERROR) << status;
            return EXIT_FAILURE;
        }
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor_ =
                std::make_shared<folly::IOThreadPoolExecutor>(std::thread::hardware_concurrency());

    std::vector<nebula::HostAddr> meta_addrs =
                nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs).value();
    nebula::meta::MetaClient metaclient(ioExecutor_, meta_addrs);

    bool loadDataOk = metaclient.waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(ERROR) << "Failed to wait for meta service ready synchronously.";
        return -1;
    }

    auto ret = metaclient.listSpaces().get();
    for (auto space : ret.value()) {
        LOG(INFO) << "list space: " << space.first << " : " << space.second;
    }

    std::vector<std::string> player_tags{"name", "age"};
    // nebula::VertexTagItem* item;
    std::vector<nebula::VertexRowItem*> rows_;

    auto ret2 = metaclient.getSpace("basketballplayer").get();
    LOG(INFO) << "space id of basketballplayer: " << ret2.value().get_space_id();

    auto ret3 = metaclient.getSpaceVidLen(ret2.value().get_space_id());
    LOG(INFO) << "spaceVidLen_: " << ret3.value();

    nebula::Value vertexId;
    vertexId.setStr("player1");

    std::string tag_name = "player";
    auto ret4 = metaclient.getTagIDByNameFromCache(ret2.value().get_space_id(), tag_name);
    LOG(INFO) << "tag id: " << ret4.value();

    auto ret5 = metaclient.getLatestTagVersionFromCache(ret2.value().get_space_id(), ret4.value());
    LOG(INFO) << "tag version: " << ret5.value();

    auto ret6 = metaclient.getTagSchemaFromCache(
                  ret2.value().get_space_id(), ret4.value(), ret5.value());
    auto schema = ret6.value();

    std::vector<std::string> names;
    size_t numFields = schema->getNumFields();
    for (size_t i = 0; i < numFields; ++i) {
        const char *propName = schema->getFieldName(i);
        names.push_back(propName);
        LOG(INFO) << propName;
    }

    auto vidTypeStatus = metaclient.getSpaceVidType(ret2.value().get_space_id());
    auto vidType = std::move(vidTypeStatus).value();

    nebula::cpp2::Value vidval;
    if (vidType == nebula::cpp2::PropertyType::INT64) {
        vidval.set_sVal(std::string(reinterpret_cast<const char*>(&vertexId.getInt()), 8));
    } else if (vidType == nebula::cpp2::PropertyType::FIXED_STRING) {
        vidval.set_sVal(vertexId.getStr());
    }

    auto status2 = metaclient.partsNum(ret2.value().get_space_id());
    auto numParts = status2.value();
    LOG(INFO) << "num parts: " << numParts;

    std::unordered_map<int32_t, nebula::HostAddr> leaders;
    for (int32_t partId = 1; partId <= numParts; ++partId) {
        auto leader = metaclient.getStorageLeaderFromCache(ret2.value().get_space_id(), partId);
        leaders[partId] = std::move(leader).value();

        LOG(INFO) << "partid: " << partId << " leader: " << leaders[partId].toString();
    }

    {
        auto part = metaclient.partId(numParts, vertexId.getStr());

        const auto& leader = leaders[part];
        LOG(INFO) << "vid key: " << vertexId.getStr() << " part: "
                  << part << " leader: " << leader.toString();

        // 初始化SstFileWriter
        rocksdb::Options options;  // 可以进行配置以优化SST文件
        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options);

        // 打开文件进行写入
        std::string file_path = "/root/sst_file.sst";
        sst_file_writer.Open(file_path);

        // 添加键值对
        // sst_file_writer.Put(nebula::NebulaKeyUtils::vertexKey(ret3.value(),
        //      part, vertexId.getStr()), "");

        std::vector<nebula::Value> props;
        nebula::Value val1;
        val1.setStr("1111");
        props.push_back(val1);
        nebula::Value val2;
        val2.setInt(100);
        props.push_back(val2);

        nebula::RowWriterV2 rowWrite(schema.get());
        for (size_t i = 0; i < names.size(); i++) {
            auto wRet = rowWrite.setValue(names[i], props[i]);
            if (wRet != nebula::WriteResult::SUCCEEDED) {
                LOG(ERROR) << "add field failed!";
            }
        }
        auto wRet = rowWrite.finish();
        if (wRet != nebula::WriteResult::SUCCEEDED) {
            LOG(ERROR) << "add field failed!";
        }

        std::string key = nebula::NebulaKeyUtils::tagKey(
                            ret3.value(), part, vertexId.getStr(), ret4.value());
        std::string value = std::move(rowWrite).moveEncodedStr();
        sst_file_writer.Put(rocksdb::Slice(key), value);

        // 完成SST文件的写入
        sst_file_writer.Finish();

        rocksdb::DB* db;
        rocksdb::Options opt;
        opt.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(opt, "/root/rocksdb", &db);

        // 摄取SST文件
        std::vector<std::string> external_sst_files = {"/root/sst_file.sst"};
        s = db->IngestExternalFile(external_sst_files, rocksdb::IngestExternalFileOptions());
        LOG(INFO) << "Ingest file status: " << s.ToString();

        std::string value2;
        s =  db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value2);
        LOG(INFO) << "Get status: " << s.ToString();

        // 关闭数据库
        delete db;
    }

    // std::vector<nebula::cpp2::Value> values;
    // nebula::cpp2::Value val1;
    // val1.set_sVal("player1");
    // nebula::cpp2::Value val2;
    // val2.set_iVal(100);

    // values.push_back( val1 );
    // values.push_back( val2 );

    return 0;
}
