//
// Created by gogobody on 2020/5/22.
//

#include "dfs_node.h"

DfsNode::DfsNode() = default;

DfsNode::~DfsNode() = default;

const std::string &DfsNode::getNodeIp() const {
    return nodeIp;
}

void DfsNode::setNodeIp(const std::string &NodeIp) {
    nodeIp = NodeIp;
}

int DfsNode::getNodePort() const {
    return nodePort;
}

void DfsNode::setNodePort(int NodePort) {
    nodePort = NodePort;
}

bool DfsNode::operator==(DfsNode &node) {
    if(is_GroupLeader!=node.is_GroupLeader){
        if(nodeIp==node.nodeIp && nodePort==node.nodePort){
            return true;
        }
    }
    return false;
}

bool DfsNode::operator==(const DfsNode &node) {
    if(is_GroupLeader!=node.is_GroupLeader){
        if(nodeIp==node.nodeIp && nodePort==node.nodePort){
            return true;
        }
    }
    return false;
}

bool DfsNode::operator==(const DfsNode &node) const {
    if(is_GroupLeader!=node.is_GroupLeader){
        if(nodeIp==node.nodeIp && nodePort==node.nodePort){
            return true;
        }
    }
    return false;
}
