/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_MONITOR_REPORT_HANDLER_H_
#define WEBSERVICE_MONITOR_REPORT_HANDLER_H_

#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "webservice/Common.h"

namespace nebula {

class MonitorReportHandler : public proxygen::RequestHandler {
 public:
  MonitorReportHandler() { }

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 protected:
  HttpCode err_{HttpCode::SUCCEEDED};
};

}  // namespace nebula
#endif  // WEBSERVICE_GETFLAGSHANDLER_H_
