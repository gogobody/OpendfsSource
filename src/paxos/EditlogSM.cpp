
#include "EditlogSM.h"
#include "dfs_types.h"

#include "nn_file_index.h"

#include "dfs_error_log.h"



PhxEditlogSM::PhxEditlogSM() : m_llCheckpointInstanceID(NoCheckpoint)
{
}

PhxEditlogSM::~PhxEditlogSM()
{
}

// propose 后执行excute
bool PhxEditlogSM::Execute(const int iGroupIdx, const uint64_t llInstanceID, 
	const string & sPaxosValue, SMCtx * poSMCtx)
{
    dfs_log_error(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, 
        "[SM Execute] ok, smid: %d, instanceid: %lu, value: %s", 
        SMID(), llInstanceID, sPaxosValue.c_str());

    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
    {
        PhxEditlogSMCtx * poPhxEditlogSMCtx = (PhxEditlogSMCtx *)poSMCtx->m_pCtx; // 继承 SMCtx
        poPhxEditlogSMCtx->iExecuteRet = NGX_OK;
        poPhxEditlogSMCtx->llInstanceID = llInstanceID; // 提议的值
        // paxos handler
		update_fi_cache_mgmt(llInstanceID, sPaxosValue, poPhxEditlogSMCtx->data);
    }
	else 
	{
        update_fi_cache_mgmt(llInstanceID, sPaxosValue, nullptr);
	}
//    printf(                  "[SM Execute] ok, smid: %d, instanceid: %lu, value: %s\n",
//                  SMID(), llInstanceID, sPaxosValue.c_str());
    return NGX_TRUE;
}

const int PhxEditlogSM::SMID() const 
{ 
    return 1; 
}

const uint64_t PhxEditlogSM::GetCheckpointInstanceID(const int iGroupIdx) const
{
    return m_llCheckpointInstanceID;
}

int PhxEditlogSM::SyncCheckpointInstanceID(const uint64_t llInstanceID)
{
    m_llCheckpointInstanceID = llInstanceID;

    return NGX_OK;
}

