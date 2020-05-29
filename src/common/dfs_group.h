
#ifndef NGXFS_DFS_GROUP_H
#define NGXFS_DFS_GROUP_H
#include <dfs_types.h>
#include <dfs_node.h>
#include <vector>

class Group{
private:
    std::vector<DfsNode> NodeList;
    DfsNode groupLeader;
    int     groupStatus;
public:
    int getGroupStatus() const;

    void setGroupStatus(int groupStatus);

public:
    const DfsNode &getGroupLeader() const;

    void setGroupLeader(const DfsNode &groupLeader);

public:
    Group();
    virtual ~Group();
    bool operator==(Group &group);
    bool operator==(const Group &group);

    const std::vector<DfsNode> &getNodeList() const;
    int                         getGroupSize();
    int                         addNode(const DfsNode &node);
    int                         removeNodeFromIp(const std::string &nodeip);
    DfsNode *                   findNodeFromip(const std::string &nodeip);


};


class DfsGroup{

public:
    DfsGroup();
    ~DfsGroup();
    static DfsGroup * getInstance(){
        if(m_instance== nullptr){
            m_instance = new DfsGroup();
        }
        return m_instance;
    }

private:
    static DfsGroup * m_instance;
    std::vector<Group> groups;
    // the group has a real master , and the groupLeader is the namenode.
    DfsNode groupMaster;
    // the whold group status
    int     globalStatus;
public:
    int getGlobalStatus() const;

    void setGlobalStatus(int globalStatus);

public:
    const DfsNode &getGroupMaster() const;

    void setGroupMaster(const DfsNode &groupMaster);


public:
    const std::vector<Group> &getGroups() const;
    int                       addGroup(const Group& group);
    int                       removeGroup(const Group& group);
    Group *                   getGroupFromLeaderIp(const std::string& nodeip);
    Group *                   findGroupFromNodeIp(const std::string& nodeip);

};



#endif //NGXFS_DFS_GROUP_H
