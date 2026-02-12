/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/MonitorReportHandler.h"

#include <folly/String.h>
#include <folly/json.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/base/Base.h"
#include "webservice/Common.h"
#include "common/monitor/MonitorReport.h"

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void MonitorReportHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (!headers->getMethod() || headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }
}

void MonitorReportHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void MonitorReportHandler::onEOM() noexcept {
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
  std::string body = monitor::monitor_report::instance()->get_format_stat();
  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::OK),
              WebServiceUtils::toString(HttpStatusCode::OK))
      .body(std::move(body))
      .sendWithEOM();
}

void MonitorReportHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void MonitorReportHandler::requestComplete() noexcept {
  delete this;
}

void MonitorReportHandler::onError(ProxygenError err) noexcept {
  LOG(ERROR) << "Web service MonitorReportHandler got error: " << proxygen::getErrorString(err);
  // delete this;
}

}  // namespace nebula
