/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/GetStatsHandler.h"

#include <folly/String.h>
#include <folly/json.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include "webservice/Common.h"
#include "common/network/NetworkUtils.h"
#include "common/nlohmann/json.hpp"

#include <time.h>

DECLARE_string(local_ip);
DECLARE_int32(port);
DECLARE_string(role);

namespace nebula {

using nebula::stats::StatsManager;
using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void GetStatsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }

  if (headers->hasQueryParam("format")) {
    returnJson_ = (headers->getQueryParam("format") == "json");
    returnMonitor_ = (headers->getQueryParam("format") == "monitor");
  }

  if (headers->hasQueryParam("stats")) {
    const std::string& stats = headers->getQueryParam("stats");
    folly::split(",", stats, statNames_, true);
  }
}

void GetStatsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void GetStatsHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    default:
      break;
  }

  // read stats
  folly::dynamic vals = getStats();
  std::string body;
  if (returnJson_) {
    body = folly::toPrettyJson(vals);
  } else if (returnMonitor_) {
    body = toMonitor(vals);
  } else {
    body = toStr(vals);
  }

  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::OK),
              WebServiceUtils::toString(HttpStatusCode::OK))
      .body(std::move(body))
      .sendWithEOM();
}

void GetStatsHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 int64_t statValue) const {
  vals.push_back(folly::dynamic::object(statName, statValue));
}

void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 const std::string& error) const {
  vals.push_back(folly::dynamic::object(statName, error));
}

void GetStatsHandler::requestComplete() noexcept { delete this; }

void GetStatsHandler::onError(ProxygenError err) noexcept {
  LOG(ERROR) << "Web service GetStatsHandler got error: " << proxygen::getErrorString(err);
  delete this;
}

folly::dynamic GetStatsHandler::getStats() const {
  auto stats = folly::dynamic::array();
  if (statNames_.empty()) {
    // Read all stats
    StatsManager::readAllValue(stats);
  } else {
    for (auto& sn : statNames_) {
      auto status = StatsManager::readValue(sn);
      if (status.ok()) {
        int64_t statValue = status.value();
        addOneStat(stats, sn, statValue);
      } else {
        addOneStat(stats, sn, status.status().toString());
      }
    }
  }
  return stats;
}

std::string GetStatsHandler::toStr(folly::dynamic& vals) const {
  std::stringstream ss;
  for (auto& counter : vals) {
    for (auto& m : counter.items()) {
      ss << m.first.asString() << "=" << m.second.asString() << "\n";
    }
  }
  return ss.str();
}

std::string GetStatsHandler::toMonitor(folly::dynamic& vals) const {
  uint64_t nowtime = ::time(NULL);
  uint64_t report_timestamp = nowtime - (nowtime % 60);

  std::string hostName;
  if (FLAGS_local_ip.empty()) {
    hostName = nebula::network::NetworkUtils::getHostname();
  } else {
    auto status = nebula::network::NetworkUtils::validateHostOrIp(FLAGS_local_ip);
    if (!status.ok()) {
      return status.message();
    }
    hostName = FLAGS_local_ip;
  }
  nebula::HostAddr localhost{hostName, FLAGS_port};

  std::ostringstream common_tag_str;
  common_tag_str << "project=nebula" << ",city=jd" << ",ip_port=" << localhost.toString()
                 << ",module=" << FLAGS_role;

  nlohmann::json stat_obj = nlohmann::json::array();
  nlohmann::json metric_obj;
  metric_obj["endpoint"] = localhost.toString();
  metric_obj["step"] = 60;
  metric_obj["counterType"] = "GAUGE";
  metric_obj["timestamp"] = report_timestamp;

  for (auto& counter : vals) {
    for (auto& m : counter.items()) {
      metric_obj["metric"] = "pv";
      metric_obj["value"] = m.second.asInt();
      metric_obj["tags"] = common_tag_str.str() + ",type=" + m.first.asString();
      stat_obj.push_back(metric_obj);
    }
  }

  return stat_obj.dump();
}

}  // namespace nebula
