#pragma once

int paxos_storage_init(char *ip, int port);

int paxos_storage_runpaxos(char *command, int len, unsigned int gid);

int paxos_storage_save_to_batch(char *command, int len, unsigned int gid,
                                unsigned int tid);

int paxos_storage_commit_batch(unsigned int tid);

int paxos_storage_process_create_group_req(unsigned int gid, char *MyIPPort,
                                           char *NodeIPPortList);

int paxos_storage_process_remove_group_req(unsigned int gid);

int paxos_storage_process_add_group_member_req(unsigned int gid,
                                               char *NodeIPPortToAdd);

int paxos_storage_process_remove_group_member_req(unsigned int gid,
                                                  char *NodeIPPortToRemove);
