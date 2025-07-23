/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

 #include "storage/http/StorageHttpListHandler.h"

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
  
  void StorageHttpListHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
   if (headers->getMethod().value() != HTTPMethod::GET) {
      // Unsupported method
      err_ = HttpCode::E_UNSUPPORTED_METHOD;
      return;
   }
 
   if (!headers->hasQueryParam("hdfspath")) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Miss argument [hdfspath]";
     return;
   }

    hdfspath_ = headers->getQueryParam("hdfspath");

    std::string local_addr = "127.0.0.1:8018";
    std::string local_path = "/nebulahdfslist?hdfspath=" + hdfspath_;

    httplib::Client cli(local_addr);
    auto response = cli.Get(local_path);

    if (response && (response->status == httplib::StatusCode::OK_200)) {
      resp_ = response->body;
    } else {
      err_ = HttpCode::E_UNPROCESSABLE;
      resp_ = httplib::to_string(response.error());
    }

    return;
  }
  
  void StorageHttpListHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
  }
  
  void StorageHttpListHandler::onEOM() noexcept {
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

    try {
        nlohmann::json json_obj = nlohmann::json::parse(resp_);
        obj["resp"] = json_obj;
    } catch (std::exception& e) {
        obj["code"] = HttpCode::E_UNPROCESSABLE;
        LOG(WARNING) << "catch exception:" << e.what();
    }

    ResponseBuilder(downstream_).status(200, "OK").body(obj.dump()).sendWithEOM();
  }
  
  void StorageHttpListHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
  }
  
  void StorageHttpListHandler::requestComplete() noexcept {
    delete this;
  }
  
  void StorageHttpListHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
  }
  
  }  // namespace storage
  }  // namespace nebula
  