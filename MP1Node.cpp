/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
//	int id = *(int*)(&memberNode->addr.addr);
//	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}


/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
//    int id = *(int*)(&memberNode->addr.addr);
//    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = false;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    memberNode->memberList.clear();

    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    doHeartbeat();
    // Check my messages
    checkMessages();
    printMemberList();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/*
 * for debug
 */
void MP1Node::printMemberList() {
    cout << "Member List Table for Node #:" << memberNode->addr.getAddress() << endl;
    for (auto &item : memberNode->memberList) {
        cout << "id:" << item.id << " port:" << item.port << " heartbeat:"
             << item.heartbeat << " timestamp:" << item.timestamp << endl;
    }
    cout << endl;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

void MP1Node::doHeartbeat(){
    this->memberNode->heartbeat++;
    for (auto &entry : memberNode->memberList) {
        if((entry.id) == int(this->memberNode->addr.addr[0])) {
            entry.setheartbeat(memberNode->heartbeat);
            entry.settimestamp(this->par->getcurrtime());
            break;
        }
    }
}

void MP1Node::combineMemberList(vector<MemberListEntry> &remoteMemberList) {
    bool found;
    for (auto &remote : remoteMemberList) {
        found = false;
        for (auto &local : this->memberNode->memberList) {
            if (local.id == remote.id) {
                found = true;
                if (local.heartbeat < remote.heartbeat) {
                    local.setheartbeat(remote.heartbeat);
                    local.settimestamp(this->par->getcurrtime());
                }
                break;
            }
        }
        // there's new member entry in the remote list sent from a live node
        if(!found && remote.gettimestamp() >=  this->par->getcurrtime() - TFAIL) {
            MemberListEntry newEntry = MemberListEntry(remote);
            this->memberNode->memberList.push_back(newEntry);
            // log node join event
            Address newAddr;
            newAddr.init();
            newAddr.addr[0] = remote.id;
            newAddr.addr[4] = remote.port;
            log->logNodeAdd(&memberNode->addr,&newAddr);
        }
    }
}

void MP1Node::removeFailedNode() {
    vector<MemberListEntry>::iterator it;
    MemberListEntry mle;
    for (it = memberNode->memberList.begin(); it < memberNode->memberList.end(); it++) {
        mle = *it;
        if (mle.gettimestamp() < this->par->getcurrtime() - TREMOVE) {
            memberNode->memberList.erase(it);
            Address removedAddr;
            removedAddr.init();
            removedAddr.addr[0] = mle.id;
            removedAddr.addr[4] = mle.port;
            log->logNodeRemove(&memberNode->addr,&removedAddr);
        }
    }
}

/*
 * pack member list into a char* buffer, structure as below:
 * listsize: int, id: int, port: short, heartbeat: long, timestamp: long
 */
void MP1Node::packMemberListToMsg(char *msg) {
    int listSize = memberNode->memberList.size();

    // pack list size
    memcpy(msg , &listSize, sizeof(int));
    int offset = sizeof(int);

    for (int i = 0; i < listSize; i++) {
        // id
        memcpy(msg + offset, &memberNode->memberList[i].id, sizeof(int));
        offset += sizeof(int);

        // port
        memcpy(msg + offset, &memberNode->memberList[i].port, sizeof(short));
        offset += sizeof(short);

        // heartbeat
        memcpy(msg + offset, &memberNode->memberList[i].heartbeat, sizeof(long));
        offset += sizeof(long);

        // timestamp
        memcpy(msg + offset, &memberNode->memberList[i].timestamp, sizeof(long));
        offset += sizeof(long);
    }
}

/*
 * uppack data from buffer, construct member list entry based on the data,
 * and then add to a vector
 */
void MP1Node::unpackMemberListFromMsg(char *msg, vector<MemberListEntry> &remoteMemberList) {
    int listSize, id;
    short port;
    long heartbeat, timestamp;

    // unpack listSize
    memcpy(&listSize, msg, sizeof(int));
    int offset = sizeof(int);

    for (int i = 0; i < listSize; i++) {
        // id
        memcpy(&id, msg + offset, sizeof(int));
        offset += sizeof(int);

        // port
        memcpy(&port, msg + offset, sizeof(short));
        offset += sizeof(short);

        // heartbeat
        memcpy(&heartbeat, msg + offset, sizeof(long));
        offset += sizeof(long);

        // timestamp
        memcpy(&timestamp,msg + offset, sizeof(long));
        offset += sizeof(long);

        MemberListEntry mle = MemberListEntry(id, port, heartbeat, timestamp);
        remoteMemberList.push_back(mle);
    }
}

void MP1Node::sendJoinRequestMsg(Address *addr) {
    size_t msgsize = sizeof(MessageHdr) + (sizeof(memberNode->addr.addr) + 1) + sizeof(long);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // Msg header
    msg->msgType = JOINREQ;
    // address
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    // heartbeat
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) + 1, &memberNode->heartbeat, sizeof(long));

    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);
    free(msg);
}

void MP1Node::sendJoinResponseMsg(Address *desAddr) {
    int listSize = memberNode->memberList.size();
    // the sender must be the starter node
//    assert(memberNode->addr == getJoinAddress());

    size_t memberListEntrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t msgsize = sizeof(MessageHdr) + (sizeof(memberNode->addr.addr) + 1) + sizeof(long) + sizeof(int) + listSize * memberListEntrySize;
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // Msgheader
    msg->msgType = JOINREP;
    // Address
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    int offset = sizeof(memberNode->addr.addr) + 1;
    // Heartbeat
    memcpy((char *)(msg+1) + offset, &memberNode->heartbeat, sizeof(long));
    offset += sizeof(long);

    // MemberList Table
    packMemberListToMsg((char *)(msg+1) + offset);

    // send JOINREP msg to the member that requst to join the group
    emulNet->ENsend(&memberNode->addr, desAddr, (char *)msg, msgsize);
    free(msg);
}

void MP1Node::sendGossipMsg() {
    int listSize = memberNode->memberList.size();

    size_t memberListEntrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t msgsize = sizeof(MessageHdr) + (sizeof(memberNode->addr.addr) + 1) + sizeof(long) + sizeof(int) + listSize * memberListEntrySize;
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // Msgheader
    msg->msgType = GOSSIP;
    // Address
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    int offset = sizeof(memberNode->addr.addr) + 1;
    // Heartbeat
    memcpy((char *)(msg+1) + offset, &memberNode->heartbeat, sizeof(long));
    offset += sizeof(long);

    packMemberListToMsg((char *)(msg+1) + offset);

    // send message to all the members in the group except self
    Address desAddress;
    for (auto &mle : memberNode->memberList) {
        // bypass the node that list as failed
        if(mle.gettimestamp() < this->par->getcurrtime() - TFAIL)
            continue;
        desAddress.init();
        desAddress.addr[0] = mle.id;
        desAddress.addr[4] = mle.port;
        if (!(memberNode->addr == desAddress)) {
            emulNet->ENsend(&memberNode->addr, &desAddress, (char *)msg, msgsize);
        }
    }
    free(msg);
}
/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        MemberListEntry mle = MemberListEntry(int(memberNode->addr.addr[0]), short(memberNode->addr.addr[4]), memberNode->heartbeat, this->par->getcurrtime());
        memberNode->memberList.push_back(mle);
        // log node join event
        Address newAddr;
        newAddr.init();
        newAddr.addr[0] = mle.id;
        newAddr.addr[4] = mle.port;
        log->logNodeAdd(&memberNode->addr,&newAddr);
    }
    else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        sendJoinRequestMsg(joinaddr);
    }
    return 1;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    // create JOINREP message: format of data is {type Address heartbeat}
    MessageHdr *inMsg;
    Address joinaddr, srcAddr;
    long heartbeat;
    vector<MemberListEntry> remoteMemberList;
    joinaddr = getJoinAddress();

    // unpack common field from data: srcAddress and heatbeat
    int offset = 0;
    inMsg = (MessageHdr *)data;
    // Address
    memcpy(&srcAddr.addr, data + sizeof(MessageHdr), sizeof(memberNode->addr.addr));
    offset += sizeof(memberNode->addr.addr) + 1;
    // heartbeat
    memcpy(&heartbeat, (data+sizeof(MessageHdr)) + offset, sizeof(long));
    offset += sizeof(long);

    if (inMsg->msgType == JOINREQ) {
        // create JOINREP message: format of data is {msgHeader, myAddress, heartbeat, myMemberListTable}
        sendJoinResponseMsg(&srcAddr);

        // log node join event
        log->logNodeAdd(&memberNode->addr,&srcAddr);
    } else if (inMsg->msgType == JOINREP) {
        // join the group
        memberNode->inGroup = true;
        memberNode->inited = true;

        // update local member entry list, add self in the list;
        MemberListEntry mle = MemberListEntry(int(memberNode->addr.addr[0]), short(memberNode->addr.addr[4]),
                                              memberNode->heartbeat, this->par->getcurrtime());
        memberNode->memberList.push_back(mle);

        // unpack the remote member list table
        unpackMemberListFromMsg(data + sizeof(MessageHdr) + offset, remoteMemberList);

        // combine local and remote member list; update neighbor number;
        combineMemberList(remoteMemberList);
        memberNode->nnb = memberNode->memberList.size() - 1;
        // Todo: set my pos in the member list
//        memberNode->myPos = std::find(memberNode->memberList.begin(), memberNode->memberList.end(), mle);
    } else if (inMsg->msgType == GOSSIP) {
        // unpack the remote member list table
        unpackMemberListFromMsg(data + sizeof(MessageHdr) + offset, remoteMemberList);

        // combine local and remote member list; update neighbor number;
        combineMemberList(remoteMemberList);
        memberNode->nnb = memberNode->memberList.size() - 1;
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
//    doHeartbeat();
    removeFailedNode();
    sendGossipMsg();
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
