
#ifndef NGXFS_DFS_NODE_H
#define NGXFS_DFS_NODE_H

#include <string>

class DfsNode{
public:
    DfsNode();

    virtual ~DfsNode();
    int         is_GroupLeader = 0;


public:
    bool operator==(DfsNode &node);
    bool operator==(const DfsNode &node);

    bool operator==(const DfsNode &node) const;

    const std::string &getNodeIp() const;

    void setNodeIp(const std::string &NodeIp);

    int getNodePort() const;

    void setNodePort(int NodePort);



private:
    int         nodePort{};
    std::string nodeIp{};
};

#endif //NGXFS_DFS_NODE_H

