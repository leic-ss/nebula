#ifndef WEBSERVICE_MONITOR_REPORT_H_
#define WEBSERVICE_MONITOR_REPORT_H_

#include <string>
#include <mutex>
#include <vector>
#include <map>
#include <thread>
#include <atomic>

namespace nebula {

namespace monitor{

enum class GraphOpType : uint32_t {
    kUnknown,
    kExplain,
    kSequential,
    kGo,
    kSet,
    kPipe,
    kUse,
    kMatch,
    kAssignment,
    kCreateTag,
    kAlterTag,
    kCreateEdge,
    kAlterEdge,
    kDescribeTag,
    kDescribeEdge,
    kCreateTagIndex,
    kCreateEdgeIndex,
    kDropTagIndex,
    kDropEdgeIndex,
    kDescribeTagIndex,
    kDescribeEdgeIndex,
    kDropTag,
    kDropEdge,
    kInsertVertices,
    kUpdateVertex,
    kInsertEdges,
    kUpdateEdge,
    kAddHosts,
    kDropHosts,
    kShowHosts,
    kShowSpaces,
    kShowParts,
    kShowTags,
    kShowEdges,
    kShowTagIndexes,
    kShowEdgeIndexes,
    kShowTagIndexStatus,
    kShowEdgeIndexStatus,
    kShowUsers,
    kShowRoles,
    kShowCreateSpace,
    kShowCreateTag,
    kShowCreateEdge,
    kShowCreateTagIndex,
    kShowCreateEdgeIndex,
    kShowSnapshots,
    kShowCharset,
    kShowCollation,
    kShowGroups,
    kShowZones,
    kShowStats,
    kShowServiceClients,
    kShowFTIndexes,
    kDescribeUser,
    kDeleteVertices,
    kDeleteTags,
    kDeleteEdges,
    kLookup,
    kCreateSpace,
    kCreateSpaceAs,
    kDropSpace,
    kDescribeSpace,
    kYield,
    kCreateUser,
    kDropUser,
    kAlterUser,
    kGrant,
    kRevoke,
    kChangePassword,
    kOrderBy,
    kShowConfigs,
    kSetConfig,
    kGetConfig,
    kFetchVertices,
    kFetchEdges,
    kFindPath,
    kLimit,
    kGroupBy,
    kReturn,
    kCreateSnapshot,
    kDropSnapshot,
    kAdminJob,
    kAdminShowJobs,
    kGetSubgraph,
    kMergeZone,
    kRenameZone,
    kDropZone,
    kDivideZone,
    kDescribeZone,
    kListZones,
    kAddHostsIntoZone,
    kAddListener,
    kRemoveListener,
    kShowListener,
    kSignInService,
    kSignOutService,
    kCreateFTIndex,
    kDropFTIndex,
    kShowSessions,
    kShowQueries,
    kKillQuery,
    kShowMetaLeader,
    kAlterSpace,
    kClearSpace,
    kUnwind,
  };


const uint32_t proc_time_interval_num = 8;
const uint32_t proc_time_interval[proc_time_interval_num] = {
  5, 10, 50, 100, 500, 1000, 2000, 5000
};

//每个请求的长度范围（Byte为单位），左闭右开区间，[)
enum StorageReqProcSizeInterval {
  PROC_SIZE0 = 0, // 0 ~ 500 
  PROC_SIZE1 = 1, // 500 ~ 1000
  PROC_SIZE2 = 2, // 1000 ~ 5000
  PROC_SIZE3 = 3, // 5000 ~ 10000
  PROC_SIZE4 = 4, // 10000 ~ 50000
  PROC_SIZE5 = 5, // 50000 ~
};

struct ServerMetricTag {
  uint64_t m_spaceid;
  GraphOpType m_operation_type;
  int32_t m_error_code;
  uint64_t m_timecost_type;
  uint64_t m_value_size_type;

  ServerMetricTag(uint64_t spaceid, GraphOpType operation_type, int32_t error_code, int32_t timecost_type, int32_t value_size_type)
    : m_spaceid(spaceid)
    , m_operation_type(operation_type)
    , m_error_code(error_code)
    , m_timecost_type(timecost_type)
    , m_value_size_type(value_size_type) {}

  std::string ToString(std::map<uint64_t, std::string>& spaceid_to_tablename) const;

  bool operator<(const ServerMetricTag &oth) const;
};

class monitor_report {
 public:
  monitor_report();
  ~monitor_report() {}

  bool initialize();
  bool start();
  bool stop();

  void update_periodic(uint64_t report_timestamp);
  std::string get_format_stat();

  void set_port(uint16_t port) { m_port = port; }
  void set_module(std::string module) { m_module = module; }
  void server_request_report(uint64_t spaceid, GraphOpType op, int32_t rc, uint64_t opTs, uint32_t opSize, int64_t num);
  uint64_t get_and_reset_request_total();
  uint64_t get_area_request_last_minute(int32_t area);

  static monitor_report* instance() { return &s_reporter; }

 private:
  bool m_running = true;
  std::thread m_thread;

  std::string m_ip;
  std::string m_project;
  uint16_t m_port;
  std::string m_module;

  std::string m_common_tag;
  std::mutex m_format_stat_mutex;
  std::string m_last_format_stat;

  static monitor_report s_reporter;

  //当单节点请求达到30wqps时，锁会严重影响性能，这里的数组是为了减小锁的粒度
  std::vector<std::mutex> m_mutex;
  std::atomic<uint64_t> m_seed{0};

  std::vector<std::map<ServerMetricTag, int64_t>> m_serverStatistic;
  std::vector<std::map<ServerMetricTag, int64_t>> m_serverTimeCostStatistic;

  uint64_t m_request_total = 0;
};

}

}

#endif