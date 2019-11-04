// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>
#include "txn/lock_manager.h"

using std::deque;

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  LockRequest request(EXCLUSIVE, txn);
  //cek udah/belum ada resource itu
  if (!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  }
  deque<LockRequest>* reqQueue = lock_table_[key];
  reqQueue->push_back(request);

  //cek udah ada yang request untuk resource itu
  if (reqQueue->size() == 1){
    return true;
  } else {
    txn_waits_[txn]+=1;
    return false;
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest>* reqQueue = lock_table_[key];
  bool hadLock = false;
  if (reqQueue->front().txn_ == txn){
    hadLock = true;
  }

  //cari & hapus request
  deque<LockRequest>::iterator i;
  for(i=reqQueue->begin(); i!=reqQueue->end(); i++){
    if(i->txn_ == txn){
      reqQueue->erase(i);
      break;
    }
  }
  
  //cari request yang akan dijalankan selanjutnya
  if(reqQueue->size() > 0 && hadLock){
    LockRequest next_req = reqQueue->front();

    Txn *txn = next_req.txn_;
    int wait = txn_waits_[txn] - 1;
    
    if(wait == 0){
      txn_waits_.erase(txn);
      ready_txns_->push_back(txn);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  owners->clear();

  //cek udah/belum ada resource itu
  if (!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  } 
  deque<LockRequest>* reqQueue = lock_table_[key];

  //cek resource itu ada yang request ato tidak
  if (reqQueue->size() == 0){
    return UNLOCKED;
  } else {
    owners->push_back(reqQueue->front().txn_);
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

