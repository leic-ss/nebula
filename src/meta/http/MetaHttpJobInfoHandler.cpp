/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/http/MetaHttpJobInfoHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/http/HttpClient.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/Common.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"

#include "common/nlohmann/json.hpp"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void MetaHttpJobInfoHandler::init(nebula::kvstore::KVStore* kvstore) {
  kvstore_ = kvstore;
  CHECK_NOTNULL(kvstore_);
}

void MetaHttpJobInfoHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  LOG(INFO) << __PRETTY_FUNCTION__;

  if (!headers->hasQueryParam("spaceid")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    resp_ = "Miss argument [spaceid]";
    return;
  }

  if (!headers->hasQueryParam("jobid")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    resp_ = "Miss argument [jobid]";
    return;
  }

  spaceid = atoi( headers->getQueryParam("spaceid").c_str() );
  jobid = atoi( headers->getQueryParam("jobid").c_str() );

  LOG(INFO) << folly::sformat("query job info spaceid {} jobid {}", spaceid, jobid);
}

void MetaHttpJobInfoHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void MetaHttpJobInfoHandler::onEOM() noexcept {
  nlohmann::json obj;
  obj["code"] = err_;
  obj["resp"] = resp_;
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .body(obj.dump())
          .sendWithEOM();
      return;
    case HttpCode::E_ILLEGAL_ARGUMENT:
      LOG(INFO) << resp_;
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST), resp_)
          .body(obj.dump())
          .sendWithEOM();
      return;
    default:
      break;
  }
  
  auto jobMgr_ = JobManager::getInstance();
  auto ret = jobMgr_->showJob(spaceid, jobid);

  if (!nebula::ok(ret)) {
    obj["code"] = nebula::error(ret);
  } else {
    obj["code"] = 0;
    obj["resp"] = (nebula::value(ret).first).get_status();
  }

  ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body(obj.dump())
        .sendWithEOM();
  return ;
}

void MetaHttpJobInfoHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void MetaHttpJobInfoHandler::requestComplete() noexcept {
  delete this;
}

void MetaHttpJobInfoHandler::onError(ProxygenError error) noexcept {
  LOG(INFO) << "Web Service MetaHttpClusterInfoHandler got error : "
            << proxygen::getErrorString(error);
}

}  // namespace meta
}  // namespace nebula
