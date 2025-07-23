/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/http/StorageHttpIngestHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
 
#include "common/process/ProcessUtils.h"
#include "webservice/Common.h"
 
#include "common/nlohmann/json.hpp"
#include "kvstore/NebulaStore.h"

 namespace nebula {
 namespace storage {
 
 using proxygen::HTTPMessage;
 using proxygen::HTTPMethod;
 using proxygen::ProxygenError;
 using proxygen::ResponseBuilder;
 using proxygen::UpgradeProtocol;
 
 void StorageHttpIngestHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
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

  if (!headers->hasQueryParam("filepath")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    resp_ = "Miss argument [filepath]";
    return;
  }

   spaceid_ = atoi( headers->getQueryParam("spaceid").c_str() );
   partid_ = atoi( headers->getQueryParam("partid").c_str() );
   filepath_ = headers->getQueryParam("filepath");

   auto* store = dynamic_cast<nebula::kvstore::NebulaStore*>(kvstore_);
   auto errOrSpace = store->space(spaceid_);
   if (!ok(errOrSpace)) {
     err_ = HttpCode::E_ILLEGAL_ARGUMENT;
     resp_ = "Space not found";
     return ;
   }

   auto space = nebula::value(errOrSpace);
   for (auto& engine : space->engines_) {
     auto parts = engine->allParts();
     for (auto part : parts) {
       if (part != partid_) continue;

       auto code = engine->ingest(std::vector<std::string>({filepath_}));
       if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
         resp_ = folly::sformat("ingest failed! spaceid {} partid {} filepath {}", spaceid_, partid_, filepath_);
         err_ = HttpCode::E_UNPROCESSABLE;
         return ;
       }

       resp_ = folly::sformat("ingest ok! spaceid {} partid {} filepath {}", spaceid_, partid_, filepath_);
       err_ = HttpCode::SUCCEEDED;
       return;
     }
   }

   resp_ = folly::sformat("ingest skip! spaceid {} partid {} filepath {}", spaceid_, partid_, filepath_);
   err_ = HttpCode::E_UNPROCESSABLE;

   return;
 }
 
 void StorageHttpIngestHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
   // Do nothing, we only support GET
 }
 
 void StorageHttpIngestHandler::onEOM() noexcept {
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
 
 void StorageHttpIngestHandler::onUpgrade(UpgradeProtocol) noexcept {
   // Do nothing
 }
 
 void StorageHttpIngestHandler::requestComplete() noexcept {
   delete this;
 }
 
 void StorageHttpIngestHandler::onError(ProxygenError error) noexcept {
   LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
 }
 
 }  // namespace storage
 }  // namespace nebula
 