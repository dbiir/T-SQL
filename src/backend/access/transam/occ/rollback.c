#include "postgres.h"

#include "access/rwset.h"
#include "tdb/kv_struct.h"
#include "tdb/timestamp_transaction/lts_generate_key.h"
#include "storage/shmem.h"
#include "access/twophase.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "cdb/cdbtm.h"
#include "tdb/tdbkvam.h"
#include "tdb/storage_param.h"

TransactionStatistics *transaction_statistics = NULL;

/* Request a piece of space in static shared memory to hold all the statistics of the current node. */
void TransactionStatisticsShmemInit(void)
{
	bool found;
	transaction_statistics = (TransactionStatistics *)
		ShmemInitStruct("Transaction Statistics", sizeof(TransactionStatistics), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		transaction_statistics->allTransactionNum = 0;
		transaction_statistics->RollBackTransactionNum = 0;
		transaction_statistics->CommitTransactionNum = 0;
		switch (dta_rollback_count)
		{
		case ROLLBACKCOUNT_10000:
			transaction_statistics->Loop = 10000;
			break;
		case ROLLBACKCOUNT_100000:
			transaction_statistics->Loop = 100000;
			break;
		case ROLLBACKCOUNT_1000000:
			transaction_statistics->Loop = 1000000;
			break;
		case ROLLBACKCOUNT_10000000:
			transaction_statistics->Loop = 10000000;
			break;
		default:
			break;
		}
		SpinLockInit(&transaction_statistics->txn_mutex);
	}
	else
	{
		Assert(found);
	}
}

Size TransactionStatisticsShmemSize(void)
{
	return sizeof(TransactionStatistics);
}

void RollBackCountAdd(void)
{
	transaction_statistics->RollBackTransactionNum++;
}

void CommitTranCountAdd(void)
{
	transaction_statistics->CommitTransactionNum++;
}

void AllTransCountAdd(void)
{
	transaction_statistics->allTransactionNum++;
	SpinLockAcquire(&transaction_statistics->txn_mutex);
	if (transaction_statistics->allTransactionNum >= transaction_statistics->Loop)
	{
		ereport(WARNING,
			(errmsg("RUCC ROLLBACK: current %d transaction, roll back %d, commit %d, rollback rate %lf",
					transaction_statistics->allTransactionNum, 
					transaction_statistics->RollBackTransactionNum, 
					transaction_statistics->CommitTransactionNum,
					(double)transaction_statistics->RollBackTransactionNum / (double)transaction_statistics->allTransactionNum)));
		transaction_statistics->allTransactionNum = 0;
		transaction_statistics->RollBackTransactionNum = 0;
		transaction_statistics->CommitTransactionNum = 0;
		switch (dta_rollback_count)
		{
		case ROLLBACKCOUNT_10000:
			transaction_statistics->Loop = 10000;
			break;
		case ROLLBACKCOUNT_100000:
			transaction_statistics->Loop = 100000;
			break;
		case ROLLBACKCOUNT_1000000:
			transaction_statistics->Loop = 1000000;
			break;
		case ROLLBACKCOUNT_10000000:
			transaction_statistics->Loop = 10000000;
			break;
		default:
			break;
		}
	}
	SpinLockRelease(&transaction_statistics->txn_mutex);
}

int GetRollBackCount(void)
{
	return transaction_statistics->RollBackTransactionNum;
}