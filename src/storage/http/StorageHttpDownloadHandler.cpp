/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

 #include "storage/http/StorageHttpDownloadHandler.h"

 #include <proxygen/httpserver/RequestHandler.h>
 #include <proxygen/httpserver/ResponseBuilder.h>
 #include <proxygen/lib/http/ProxygenErrorEnum.h>
  
 #include "common/process/ProcessUtils.h"
 #include "webservice/Common.h"
 
 #include "common/fs/FileUtils.h"
 #include "folly/Format.h"

 #include "common/nlohmann/json.hpp"
 #include "common/cpphttplib/httplib.h"
 #include "kvstore/NebulaStore.h"
 
  namespace nebula {
  namespace storage {
  
  using proxygen::HTTPMessage;
  using proxygen::HTTPMethod;
  using proxygen::ProxygenError;
  using proxygen::ResponseBuilder;
  using proxygen::UpgradeProtocol;
  
  void StorageHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
   if (headers->getMethod().value() != HTTPMethod::GET) {
      // Unsupported method
      err_ = HttpCode::E_UNSUPPORTED_METHOD;
      return;
   }
   if (kvstore_ == nullptr) {
      err_ = HttpCode::SUCCEEDED;
      resp_ = folly::stringPrintf("Error inside");
      return;
   }
 
   if (!headers->hasQueryParam("spaceid")) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Miss argument [spaceid]";
     return;
   }
 
   if (!headers->hasQueryParam("partid")) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Miss argument [partid]";
     return;
   }
 
   if (!headers->hasQueryParam("hdfspath")) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Miss argument [hdfspath]";
     return;
   }
 
    spaceid_ = atoi( headers->getQueryParam("spaceid").c_str() );
    partid_ = atoi( headers->getQueryParam("partid").c_str() );
    hdfspath_ = headers->getQueryParam("hdfspath");
 
    auto* store = dynamic_cast<nebula::kvstore::NebulaStore*>(kvstore_);
    auto partResult = store->part(spaceid_, partid_);
    if (!ok(partResult)) {
        LOG(ERROR) << "Can't found space: " << spaceid_ << ", part: " << partid_;
        err_ = HttpCode::E_UNPROCESSABLE;
        resp_ = folly::sformat("Can't found space! spaceid {} partid {} hdfspath {}", spaceid_, partid_, hdfspath_);
        return ;
    }

    auto localPath = folly::stringPrintf("%s/download", value(partResult)->engine()->getDataRoot());
    if (nebula::fs::FileUtils::fileType(localPath.c_str()) == fs::FileType::NOTEXIST) {
        if (!nebula::fs::FileUtils::makeDir(localPath)) {
            err_ = HttpCode::E_UNPROCESSABLE;
            resp_ = folly::sformat("task executaion failed! spaceid {} partid {} hdfspath {}", spaceid_, partid_, hdfspath_);
            return ;
        }
    }

    std::string local_addr = "127.0.0.1:8018";
    std::string local_path = "/nebuladownload?hdfspath=" + hdfspath_ + "&localdir=" + localPath;

    httplib::Client cli(local_addr);
    auto response = cli.Get(local_path);
  
    std::string filename = hdfspath_.substr( hdfspath_.find_last_of('/') + 1);
    if (response && (response->status == httplib::StatusCode::OK_200)) {
      resp_ = localPath + "/" + filename;
    } else {
      err_ = HttpCode::E_UNPROCESSABLE;
      resp_ = httplib::to_string(response.error());
    }

    return;
  }
  
  void StorageHttpDownloadHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
  }
  
  void StorageHttpDownloadHandler::onEOM() noexcept {
    nlohmann::json obj;
    obj["code"] = err_;
    obj["resp"] = resp_;
    switch (err_) {
      case HttpCode::E_UNSUPPORTED_METHOD:
        ResponseBuilder(downstream_).status(405, "Method Not Allowed").body(obj.dump()).sendWithEOM();
        return;
      case HttpCode::E_ILLEGAL_ARGUMENT:
        ResponseBuilder(downstream_).status(400, "Bad Request").body(obj.dump()).sendWithEOM();
        return;
      case HttpCode::E_UNPROCESSABLE:
        ResponseBuilder(downstream_).status(401, "Process Failed").body(obj.dump()).sendWithEOM();
        return;
      default:
        break;
    }
 
    ResponseBuilder(downstream_).status(200, "OK").body(obj.dump()).sendWithEOM();
  }
  
  void StorageHttpDownloadHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
  }
  
  void StorageHttpDownloadHandler::requestComplete() noexcept {
    delete this;
  }
  
  void StorageHttpDownloadHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
  }
  
  }  // namespace storage
  }  // namespace nebula
  