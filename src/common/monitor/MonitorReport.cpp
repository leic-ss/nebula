#include <unistd.h>

#include <sys/socket.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <net/if.h>
#include <sstream>
#include <thread>

#include "common/monitor/MonitorReport.h"
#include "common/nlohmann/json.hpp"

#define REPORT_VECTOR_SIZE 128

namespace nebula {

namespace monitor {

monitor_report monitor_report::s_reporter;

static int32_t getOpProcTsIdx(uint32_t opTs)
{
  uint32_t idx = 0;
  do {
    if (opTs < proc_time_interval[idx]) return idx;
  } while (++idx < proc_time_interval_num);

  return idx;
}

static int32_t getOpProcSizeIdx(uint32_t opSize)
{
  int32_t idx = PROC_SIZE0;
  if (opSize >= 50000) {
    idx = PROC_SIZE5;
  } else if (opSize >= 10000) {
    idx = PROC_SIZE4;
  } else if (opSize >= 5000) {
    idx = PROC_SIZE3;
  } else if (opSize >= 1000) {
    idx = PROC_SIZE2;
  } else if (opSize >= 100) {
    idx = PROC_SIZE1;
  } else {
    idx = PROC_SIZE0;
  }

  return idx;
}

static uint32_t getLocalIp()
{
    uint32_t ip;
    int32_t fd, intrface;
    struct ifreq buf[32];
    struct ifconf ifc;
    ip = -1;
    if ((fd = socket (AF_INET, SOCK_DGRAM, 0)) >= 0) {
        ifc.ifc_len = sizeof buf;
        ifc.ifc_buf = (caddr_t) buf;
        if (!ioctl (fd, SIOCGIFCONF, (char *) &ifc)) {
            intrface = ifc.ifc_len / sizeof (struct ifreq); 
            while (intrface-- > 0) {
                if (!(ioctl (fd, SIOCGIFADDR, (char *) &buf[intrface]))) {
                    ip = inet_addr( inet_ntoa( ((struct sockaddr_in*)(&buf[intrface].ifr_addr))->sin_addr) );
                    break;
                }
            }
        }
        close(fd);
    }
    return ip;
}

static std::string addr2String(uint64_t ipport)
{
    char str[32];
    uint32_t ip = (uint32_t)(ipport & 0xffffffff);
    int port = (int)((ipport >> 32 ) & 0xffff);
    unsigned char *bytes = (unsigned char *) &ip;
    if (port > 0) {
        sprintf(str, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
    } else {
        sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
    }
    return str;
}

static std::string GraphOpTypeToString(GraphOpType op_type)
{
  char str[64] = {0};
  switch (op_type) {
    case GraphOpType::kUnknown:
      snprintf(str, 63, "kUnknown"); break;
    case GraphOpType::kExplain:
      snprintf(str, 63, "kExplain"); break;
    case GraphOpType::kSequential:
      snprintf(str, 63, "kSequential"); break;
    case GraphOpType::kGo:
      snprintf(str, 63, "kGo"); break;
    case GraphOpType::kSet:
      snprintf(str, 63, "kSet"); break;
    case GraphOpType::kPipe:
      snprintf(str, 63, "kPipe"); break;
    case GraphOpType::kUse:
      snprintf(str, 63, "kUse"); break;
    case GraphOpType::kMatch:
      snprintf(str, 63, "kMatch"); break;
    case GraphOpType::kAssignment:
      snprintf(str, 63, "kAssignment"); break;
    case GraphOpType::kCreateTag:
      snprintf(str, 63, "kCreateTag"); break;
    case GraphOpType::kAlterTag:
      snprintf(str, 63, "kAlterTag"); break;
    case GraphOpType::kCreateEdge:
      snprintf(str, 63, "kCreateEdge"); break;
    case GraphOpType::kAlterEdge:
      snprintf(str, 63, "kAlterEdge"); break;
    case GraphOpType::kDescribeTag:
      snprintf(str, 63, "kDescribeTag"); break;
    case GraphOpType::kDescribeEdge:
      snprintf(str, 63, "kDescribeEdge"); break;
    case GraphOpType::kCreateTagIndex:
      snprintf(str, 63, "kCreateTagIndex"); break;
    case GraphOpType::kCreateEdgeIndex:
      snprintf(str, 63, "kCreateEdgeIndex"); break;
    case GraphOpType::kDropTagIndex:
      snprintf(str, 63, "kDropTagIndex"); break;
    case GraphOpType::kDropEdgeIndex:
      snprintf(str, 63, "kDropEdgeIndex"); break;
    case GraphOpType::kDescribeTagIndex:
      snprintf(str, 63, "kDescribeTagIndex"); break;
    case GraphOpType::kDescribeEdgeIndex:
      snprintf(str, 63, "kDescribeEdgeIndex"); break;
    case GraphOpType::kDropTag:
      snprintf(str, 63, "kDropTag"); break;
    case GraphOpType::kDropEdge:
      snprintf(str, 63, "kDropEdge"); break;
    case GraphOpType::kInsertVertices:
      snprintf(str, 63, "kInsertVertices"); break;
    case GraphOpType::kUpdateVertex:
      snprintf(str, 63, "kUpdateVertex"); break;
    case GraphOpType::kInsertEdges:
      snprintf(str, 63, "kInsertEdges"); break;
    case GraphOpType::kUpdateEdge:
      snprintf(str, 63, "kUpdateEdge"); break;
    case GraphOpType::kAddHosts:
      snprintf(str, 63, "kAddHosts"); break;
    case GraphOpType::kDropHosts:
      snprintf(str, 63, "kDropHosts"); break;
    case GraphOpType::kShowHosts:
      snprintf(str, 63, "kShowHosts"); break;
    case GraphOpType::kShowSpaces:
      snprintf(str, 63, "kShowSpaces"); break;
    case GraphOpType::kShowParts:
      snprintf(str, 63, "kShowParts"); break;
    case GraphOpType::kShowTags:
      snprintf(str, 63, "kShowTags"); break;
    case GraphOpType::kShowEdges:
      snprintf(str, 63, "kShowEdges"); break;
    case GraphOpType::kShowTagIndexes:
      snprintf(str, 63, "kShowTagIndexes"); break;
    case GraphOpType::kShowEdgeIndexes:
      snprintf(str, 63, "kShowEdgeIndexes"); break;
    case GraphOpType::kShowTagIndexStatus:
      snprintf(str, 63, "kShowTagIndexStatus"); break;
    case GraphOpType::kShowEdgeIndexStatus:
      snprintf(str, 63, "kShowEdgeIndexStatus"); break;
    case GraphOpType::kShowUsers:
      snprintf(str, 63, "kShowUsers"); break;
    case GraphOpType::kShowRoles:
      snprintf(str, 63, "kShowRoles"); break;
    case GraphOpType::kShowCreateSpace:
      snprintf(str, 63, "kShowCreateSpace"); break;
    case GraphOpType::kShowCreateTag:
      snprintf(str, 63, "kShowCreateTag"); break;
    case GraphOpType::kShowCreateEdge:
      snprintf(str, 63, "kShowCreateEdge"); break;
    case GraphOpType::kShowCreateTagIndex:
      snprintf(str, 63, "kShowCreateTagIndex"); break;
    case GraphOpType::kShowCreateEdgeIndex:
      snprintf(str, 63, "kShowCreateEdgeIndex"); break;
    case GraphOpType::kShowSnapshots:
      snprintf(str, 63, "kShowSnapshots"); break;
    case GraphOpType::kShowCharset:
      snprintf(str, 63, "kShowCharset"); break;
    case GraphOpType::kShowCollation:
      snprintf(str, 63, "kShowCollation"); break;
    case GraphOpType::kShowGroups:
      snprintf(str, 63, "kShowGroups"); break;
    case GraphOpType::kShowZones:
      snprintf(str, 63, "kShowZones"); break;
    case GraphOpType::kShowStats:
      snprintf(str, 63, "kShowStats"); break;
    case GraphOpType::kShowServiceClients:
      snprintf(str, 63, "kShowServiceClients"); break;
    case GraphOpType::kShowFTIndexes:
      snprintf(str, 63, "kShowFTIndexes"); break;
    case GraphOpType::kDescribeUser:
      snprintf(str, 63, "kDescribeUser"); break;
    case GraphOpType::kDeleteVertices:
      snprintf(str, 63, "kDeleteVertices"); break;
    case GraphOpType::kDeleteTags:
      snprintf(str, 63, "kDeleteTags"); break;
    case GraphOpType::kDeleteEdges:
      snprintf(str, 63, "kDeleteEdges"); break;
    case GraphOpType::kLookup:
      snprintf(str, 63, "kLookup"); break;
    case GraphOpType::kCreateSpace:
      snprintf(str, 63, "kCreateSpace"); break;
    case GraphOpType::kCreateSpaceAs:
      snprintf(str, 63, "kCreateSpaceAs"); break;
    case GraphOpType::kDropSpace:
      snprintf(str, 63, "kDropSpace"); break;
    case GraphOpType::kDescribeSpace:
      snprintf(str, 63, "kDescribeSpace"); break;
    case GraphOpType::kYield:
      snprintf(str, 63, "kYield"); break;
    case GraphOpType::kCreateUser:
      snprintf(str, 63, "kCreateUser"); break;
    case GraphOpType::kDropUser:
      snprintf(str, 63, "kDropUser"); break;
    case GraphOpType::kAlterUser:
      snprintf(str, 63, "kAlterUser"); break;
    case GraphOpType::kGrant:
      snprintf(str, 63, "kGrant"); break;
    case GraphOpType::kRevoke:
      snprintf(str, 63, "kRevoke"); break;
    case GraphOpType::kChangePassword:
      snprintf(str, 63, "kChangePassword"); break;
    case GraphOpType::kOrderBy:
      snprintf(str, 63, "kOrderBy"); break;
    case GraphOpType::kShowConfigs:
      snprintf(str, 63, "kShowConfigs"); break;
    case GraphOpType::kSetConfig:
      snprintf(str, 63, "kSetConfig"); break;
    case GraphOpType::kGetConfig:
      snprintf(str, 63, "kGetConfig"); break;
    case GraphOpType::kFetchVertices:
      snprintf(str, 63, "kFetchVertices"); break;
    case GraphOpType::kFetchEdges:
      snprintf(str, 63, "kFetchEdges"); break;
    case GraphOpType::kFindPath:
      snprintf(str, 63, "kFindPath"); break;
    case GraphOpType::kLimit:
      snprintf(str, 63, "kLimit"); break;
    case GraphOpType::kGroupBy:
      snprintf(str, 63, "kGroupBy"); break;
    case GraphOpType::kReturn:
      snprintf(str, 63, "kReturn"); break;
    case GraphOpType::kCreateSnapshot:
      snprintf(str, 63, "kCreateSnapshot"); break;
    case GraphOpType::kDropSnapshot:
      snprintf(str, 63, "kDropSnapshot"); break;
    case GraphOpType::kAdminJob:
      snprintf(str, 63, "kAdminJob"); break;
    case GraphOpType::kAdminShowJobs:
      snprintf(str, 63, "kAdminShowJobs"); break;
    case GraphOpType::kGetSubgraph:
      snprintf(str, 63, "kGetSubgraph"); break;
    case GraphOpType::kMergeZone:
      snprintf(str, 63, "kMergeZone"); break;
    case GraphOpType::kRenameZone:
      snprintf(str, 63, "kRenameZone"); break;
    case GraphOpType::kDropZone:
      snprintf(str, 63, "kDropZone"); break;
    case GraphOpType::kDivideZone:
      snprintf(str, 63, "kDivideZone"); break;
    case GraphOpType::kDescribeZone:
      snprintf(str, 63, "kDescribeZone"); break;
    case GraphOpType::kListZones:
      snprintf(str, 63, "kListZones"); break;
    case GraphOpType::kAddHostsIntoZone:
      snprintf(str, 63, "kAddHostsIntoZone"); break;
    case GraphOpType::kAddListener:
      snprintf(str, 63, "kAddListener"); break;
    case GraphOpType::kRemoveListener:
      snprintf(str, 63, "kRemoveListener"); break;
    case GraphOpType::kShowListener:
      snprintf(str, 63, "kShowListener"); break;
    case GraphOpType::kSignInService:
      snprintf(str, 63, "kSignInService"); break;
    case GraphOpType::kSignOutService:
      snprintf(str, 63, "kSignOutService"); break;
    case GraphOpType::kCreateFTIndex:
      snprintf(str, 63, "kCreateFTIndex"); break;
    case GraphOpType::kDropFTIndex:
      snprintf(str, 63, "kDropFTIndex"); break;
    case GraphOpType::kShowSessions:
      snprintf(str, 63, "kShowSessions"); break;
    case GraphOpType::kShowQueries:
      snprintf(str, 63, "kShowQueries"); break;
    case GraphOpType::kKillQuery:
      snprintf(str, 63, "kKillQuery"); break;
    case GraphOpType::kShowMetaLeader:
      snprintf(str, 63, "kShowMetaLeader"); break;
    case GraphOpType::kAlterSpace:
      snprintf(str, 63, "kAlterSpace"); break;
    case GraphOpType::kClearSpace:
      snprintf(str, 63, "kClearSpace"); break;
    case GraphOpType::kUnwind:
      snprintf(str, 63, "kUnwind"); break;
    default:
      snprintf(str, 63, "default");
      break;
  }

  return str;
}

static std::string TimeIntervalToString(uint32_t time_interval)
{
  char str[32];
  if (time_interval <= 0) {
    snprintf(str, 31, "0_%d", proc_time_interval[0]);
  } else if (time_interval < proc_time_interval_num) {
    snprintf(str, 31, "%d_%d", proc_time_interval[time_interval - 1], proc_time_interval[time_interval]);
  } else {
    snprintf(str, 31, "%d_more", proc_time_interval[proc_time_interval_num - 1]);
  }

  return str;
}

static std::string SizeIntervalToString(int32_t size_interval)
{
  char str[64] = {0};
  switch (size_interval) {
    case PROC_SIZE0:
      snprintf(str, 63, "0_500B");
      break;
    case PROC_SIZE1:
      snprintf(str, 63, "500_1000B");
      break;
    case PROC_SIZE2:
      snprintf(str, 63, "1_5KB");
      break;
    case PROC_SIZE3:
      snprintf(str, 63, "5_10KB");
      break;
    case PROC_SIZE4:
      snprintf(str, 63, "10_50KB");
      break;
    default:
      snprintf(str, 63, "unknown");
      break;
  }

  return str;
}

std::string ServerMetricTag::ToString(std::map<uint64_t, std::string>& spaceid_to_name) const
{
  std::ostringstream oss;
  std::string space_str;
  if (spaceid_to_name.count(m_spaceid)) {
    space_str = spaceid_to_name[m_spaceid] + "(" + std::to_string(m_spaceid) + ")";
  } else {
    space_str = std::to_string(m_spaceid);
  }
  oss << "spaceid=" << space_str << ",operation=" << GraphOpTypeToString(m_operation_type)
    << ",elaspe=" << TimeIntervalToString(m_timecost_type) << ",code=" << m_error_code
    << ",value_size=" << SizeIntervalToString(m_value_size_type);
  return oss.str();
}

bool ServerMetricTag::operator<(const ServerMetricTag &oth) const
{
  if (m_spaceid != oth.m_spaceid) {
    return m_spaceid < oth.m_spaceid;
  } else if (m_operation_type != oth.m_operation_type) {
    return m_operation_type < oth.m_operation_type;
  } else if (m_timecost_type != oth.m_timecost_type) {
    return m_timecost_type < oth.m_timecost_type;
  } else if (m_error_code != oth.m_error_code) {
    return m_error_code < oth.m_error_code;
  }
  return m_value_size_type < oth.m_value_size_type;
}

monitor_report::monitor_report() : m_project("nebula"), m_mutex(REPORT_VECTOR_SIZE) {}

bool monitor_report::initialize()
{
  m_ip = addr2String( getLocalIp() );
  char str[1024];
  snprintf(str, 1023, "project=%s,ip_port=%s:%d", m_project.c_str(), m_ip.c_str(), m_port);
  m_common_tag = str;

  m_serverStatistic.resize(REPORT_VECTOR_SIZE);
  m_serverTimeCostStatistic.resize(REPORT_VECTOR_SIZE);

  return true;
}

bool monitor_report::start()
{
  m_thread = std::thread([this]() {
    while (m_running) {
      uint64_t nowtime = time(NULL);
      if (nowtime % 60 == 0) {
        uint64_t last_timestamp = nowtime - 60;
        update_periodic(last_timestamp);
      }
      sleep(1);
    }
  });
  return true;
}

bool monitor_report::stop()
{
    m_running = false;
    if (m_thread.joinable()) m_thread.join();
    return true;
}

void monitor_report::server_request_report(uint64_t spaceid, GraphOpType op, int32_t rc, uint64_t opTs, uint32_t opSize, int64_t num)
{
  ServerMetricTag serverMetricTag(spaceid, op, rc, getOpProcTsIdx(opTs), getOpProcSizeIdx(opSize));

  m_seed++;
  int32_t index = m_seed.load() % REPORT_VECTOR_SIZE;

  std::lock_guard<std::mutex> guard(m_mutex[index]);
  m_serverStatistic[index][serverMetricTag] += num;
  m_serverTimeCostStatistic[index][serverMetricTag] += opTs;
  m_request_total += num;
}

uint64_t monitor_report::get_and_reset_request_total() {
  uint64_t request_total = m_request_total;
  m_request_total = 0;
  return request_total;
}

void monitor_report::update_periodic(uint64_t report_timestamp)
{

  std::map<uint64_t, std::string> spaceid_to_tablename;

  std::vector<std::map<ServerMetricTag, int64_t>> serverStatistic(REPORT_VECTOR_SIZE);
  std::vector<std::map<ServerMetricTag, int64_t>> serverTimeCostStatistic(REPORT_VECTOR_SIZE);

  for (int idx = 0; idx < REPORT_VECTOR_SIZE; ++idx) {
    std::lock_guard<std::mutex> guard(m_mutex[idx]);
    serverStatistic[idx].swap(m_serverStatistic[idx]);
    serverTimeCostStatistic[idx].swap(m_serverTimeCostStatistic[idx]);
  }

  std::map<ServerMetricTag, int64_t> serverStatistic_all;
  std::map<ServerMetricTag, int64_t> serverTimeCostStatistic_all;

  for (int idx = 0; idx < REPORT_VECTOR_SIZE; ++idx) {
    for (auto& it : serverStatistic[idx]) {
      serverStatistic_all[it.first] += it.second;
    }
    for (auto& it : serverTimeCostStatistic[idx]) {
      serverTimeCostStatistic_all[it.first] += it.second;
    }
  }

  char tags[1024];
  nlohmann::json stat_json = nlohmann::json::array();
  nlohmann::json metric_json;
  metric_json["step"] = 60;
  metric_json["endpoint"] = m_ip;
  metric_json["counterType"] = "GAUGE";
  metric_json["timestamp"] = report_timestamp;

  for (const auto &metric_iter : serverStatistic_all) {
    metric_json["metric"] = "pv";
    metric_json["value"] = metric_iter.second;
    metric_json["tags"] = m_common_tag + ",module=" + m_module + "," + metric_iter.first.ToString(spaceid_to_tablename);
    stat_json.push_back(metric_json);
  }
  for (const auto &metric_iter : serverTimeCostStatistic_all) {
    metric_json["metric"] = "consume_time";
    metric_json["value"] = metric_iter.second;
    metric_json["tags"] = m_common_tag + ",module=" + m_module + "," + metric_iter.first.ToString(spaceid_to_tablename);
    stat_json.push_back(metric_json);
  }

  std::string format_stat = stat_json.dump();
  do {
    std::lock_guard<std::mutex> guard(m_format_stat_mutex);
    m_last_format_stat = format_stat;
  } while(0);

  return ;
}

std::string monitor_report::get_format_stat()
{
  std::lock_guard<std::mutex> guard(m_format_stat_mutex);
  return m_last_format_stat;
}

}
}

// int32_t main()
// {
//   return 0;
// }