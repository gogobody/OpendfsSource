#ifndef FS_EDIT_LOG_H_
#define FS_EDIT_LOG_H_

#include "phxpaxos/node.h"
#include "EditlogSM.h"
#include <string>
#include <vector>

using namespace phxpaxos;
using namespace std;

class FSEditlog
{
public:
    FSEditlog(const NodeInfo & oMyNode, const NodeInfoList & vecNodeList, 
        string & sPaxosLogPath, int iGroupCount);
    ~FSEditlog();

    void setCheckpointInstanceID(const uint64_t llInstanceID);
	
    int RunPaxos();

    const NodeInfo GetMaster(const string & sKey);
    const bool IsIMMaster(const string & sKey);

	int Propose(const string & sKey, const string & sPaxosValue, 
        PhxEditlogSMCtx & oEditlogSMCtx);

private:
    int MakeLogStoragePath(string & sLogStoragePath);
    int GetGroupIdx(const string & sKey);
    
private:
    NodeInfo m_oMyNode; //oMyNode标识本机的IP/PORT信息
    NodeInfoList m_vecNodeList; //vecNodeList标识多副本集群的所有机器信息
    string m_sPaxosLogPath;
    int m_iGroupCount;

    Node * m_poPaxosNode; //本次我们需要运行的PhxPaxos实例指针
    PhxEditlogSM m_oEditlogSM; //刚刚编写的状态机类
};

#endif

