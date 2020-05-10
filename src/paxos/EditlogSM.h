#ifndef EDIT_LOG_SM_H_
#define EDIT_LOG_SM_H_

#include "phxpaxos/sm.h"
#include "phxpaxos/options.h"
#include <stdio.h>
#include <unistd.h>

using namespace phxpaxos;
using namespace std;
//Echo上下文数据类型定义：
class PhxEditlogSMCtx
{
public:
    int iExecuteRet;
	uint64_t llInstanceID;
	void *data;

    PhxEditlogSMCtx()
    {
        iExecuteRet = -1;//iExecuteRet可以获得Execute的执行情况
		llInstanceID = 0;
		data = nullptr;
    }
};

// 状态机
class PhxEditlogSM : public StateMachine
{
public:
    PhxEditlogSM();
    ~PhxEditlogSM();

    //状态转移函数 param sPaxosValue
    //函数的参数出现了一个陌生的类型SMCtx
    bool Execute(const int iGroupIdx, const uint64_t llInstanceID, 
            const string & sPaxosValue, SMCtx * poSMCtx);

    //返回这个状态机的唯一标识ID
    const int SMID() const;

    const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const;
    int SyncCheckpointInstanceID(const uint64_t llInstanceID);

private:
    uint64_t m_llCheckpointInstanceID;
};

#endif

