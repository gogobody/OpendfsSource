
#include "dfs_group.h"


Group::Group() = default;

Group::~Group() = default;


const std::vector<DfsNode> &Group::getNodeList() const {
    return NodeList;
}

int Group::addNode(const DfsNode &node) {
    NodeList.push_back(node);
    return NGX_OK;
}

int Group::removeNodeFromIp(const std::string &nodeip) {
    auto iter = NodeList.begin();
    while(iter!=NodeList.end()){
        if((*iter).getNodeIp() == nodeip){
            NodeList.erase(iter);
            return NGX_OK;
        }
    }
    return NGX_ERROR;
}

const DfsNode &Group::getGroupLeader() const {
    return groupLeader;
}

void Group::setGroupLeader(const DfsNode &groupLeader) {
    Group::groupLeader = groupLeader;
}


DfsNode *Group::findNodeFromip(const std::string &nodeip) {
    auto iter = NodeList.begin();
    while(iter!=NodeList.end()){
        if((*iter).getNodeIp() == nodeip){
            return &*iter;
        }
    }
    return nullptr;
}

bool Group::operator==(Group &group) {
    if(groupLeader == group.groupLeader){
        if(NodeList == group.NodeList){
            return true;
        }
    }

    return false;
}

bool Group::operator==(const Group &group) {
    if(groupLeader == group.groupLeader){
        if(NodeList == group.NodeList){
            return true;
        }
    }

    return false;
}

int Group::getGroupSize() {
    return NodeList.size();
}

int Group::getGroupStatus() const {
    return groupStatus;
}

void Group::setGroupStatus(int groupStatus) {
    Group::groupStatus = groupStatus;
}


// dfs group

DfsGroup::DfsGroup() = default;

DfsGroup::~DfsGroup() = default;
DfsGroup* DfsGroup::m_instance = nullptr;

const std::vector<Group> &DfsGroup::getGroups() const {
    return groups;
}

int DfsGroup::addGroup(const Group &group) {
    groups.push_back(group);
    return NGX_OK;
}

int DfsGroup::removeGroup(const Group &group) {
    auto iter = groups.begin();
    while (iter!=groups.end()){
        if((*iter) == group){
            groups.erase(iter);
            return NGX_OK;
        }
    }
    return NGX_ERROR;
}

Group *DfsGroup::getGroupFromLeaderIp(const std::string& nodeip) {
    auto iter = groups.begin();
    while (iter!=groups.end()){
        if((*iter).getGroupLeader().getNodeIp() == nodeip){
            return &*iter;
        }
    }
    return nullptr;
}

// 默认一个数据节点只属于一个群组
Group *DfsGroup::findGroupFromNodeIp(const std::string& nodeip) {
    auto iter = groups.begin();
    while (iter!=groups.end()){
        DfsNode *node= nullptr;
        node = (*iter).findNodeFromip(nodeip);
        if (node != nullptr){
            return &*iter;
        }
    }
    return nullptr;
}

const DfsNode &DfsGroup::getGroupMaster() const {
    return groupMaster;
}

void DfsGroup::setGroupMaster(const DfsNode &groupMaster) {
    DfsGroup::groupMaster = groupMaster;
}

int DfsGroup::getGlobalStatus() const {
    return globalStatus;
}

void DfsGroup::setGlobalStatus(int globalStatus) {
    DfsGroup::globalStatus = globalStatus;
}




