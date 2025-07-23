/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

 #include "storage/http/StorageHttpCheckHandler.h"

 #include <proxygen/httpserver/RequestHandler.h>
 #include <proxygen/httpserver/ResponseBuilder.h>
 #include <proxygen/lib/http/ProxygenErrorEnum.h>
  
 #include "common/process/ProcessUtils.h"
 #include "webservice/Common.h"

 #include "common/fs/FileUtils.h"

 #include "common/nlohmann/json.hpp"
 #include "kvstore/NebulaStore.h"
 
  namespace nebula {
  namespace storage {
  
  using proxygen::HTTPMessage;
  using proxygen::HTTPMethod;
  using proxygen::ProxygenError;
  using proxygen::ResponseBuilder;
  using proxygen::UpgradeProtocol;
  
  void StorageHttpCheckHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
   if (headers->getMethod().value() != HTTPMethod::GET) {
      // Unsupported method
      err_ = HttpCode::E_UNSUPPORTED_METHOD;
      return;
   }
 
   if (!headers->hasQueryParam("filepath")) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Miss argument [filepath]";
     return;
   }
 
    filepath_ = headers->getQueryParam("filepath");

    if (nebula::fs::FileUtils::fileType(filepath_.c_str()) == fs::FileType::NOTEXIST) {
        err_ = HttpCode::E_UNPROCESSABLE;
        resp_ = folly::sformat("file {} is not exist", filepath_);
        return ;
    }
 
    resp_ = folly::sformat("file {} is exist", filepath_);
    return;
  }
  
  void StorageHttpCheckHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
  }
  
  void StorageHttpCheckHandler::onEOM() noexcept {
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
  
  void StorageHttpCheckHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
  }
  
  void StorageHttpCheckHandler::requestComplete() noexcept {
    delete this;
  }
  
  void StorageHttpCheckHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
  }
  
  }  // namespace storage
  }  // namespace nebula
  