package com.example.springbatchkotlin.job

import com.example.springbatchkotlin.domain.account.Account
import com.example.springbatchkotlin.infrastructure.persistence.jpa.account.AccountEntity
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemWriter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class AccountJobConfig(
    private val entityManagerFactory: EntityManagerFactory,
    private val jobRepository: JobRepository,
) {

    private val chunkSize = 3

    @Bean
    fun accountJob(accountStep: Step): Job {
        return JobBuilder("accountJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .start(accountStep)
            .listener(JobDurationTrackerListener())
            .build()
    }

//    @Bean
//    fun accountStep(
//        transactionManager: PlatformTransactionManager,
//        accountReader: ItemReader<AccountEntity>,
//        accountProcessor: ItemProcessor<AccountEntity, Account>,
//        accountWriter: ItemWriter<Account>,
//    ): Step {
//        return StepBuilder("accountStep", jobRepository)
//            .chunk<AccountEntity, Account>(chunkSize, transactionManager)
//            .listener(StepDurationTrackerListener())
//            .reader(accountReader)
//            .processor(accountProcessor)
//            .writer(accountWriter)
//            .listener(ChunkDurationTrackerListener())
//            .build()
//    }

    @Bean
    fun accountStep(
        transactionManager: PlatformTransactionManager,
        noOffsetAccountReader: NoOffsetAccountReader<AccountEntity>,
        accountProcessor: ItemProcessor<AccountEntity, Account>,
        accountWriter: ItemWriter<Account>,
    ): Step {
        return StepBuilder("accountStep", jobRepository)
            .chunk<AccountEntity, Account>(chunkSize, transactionManager)
            .listener(StepDurationTrackerListener())
            .reader(noOffsetAccountReader)
            .processor(accountProcessor)
            .writer(accountWriter)
            .listener(ChunkDurationTrackerListener())
            .build()
    }

//    @Bean
//    fun accountStep(
//        transactionManager: PlatformTransactionManager,
//        jpaCursorAccountReader: JpaCursorItemReader<AccountEntity>,
//        accountProcessor: ItemProcessor<AccountEntity, Account>,
//        accountWriter: ItemWriter<Account>
//    ): Step {
//        return StepBuilder("accountStep", jobRepository)
//            .chunk<AccountEntity, Account>(chunkSize, transactionManager)
//            .listener(StepDurationTrackerListener())
//            .reader(jpaCursorAccountReader)
//            .processor(accountProcessor)
//            .writer(accountWriter)
//            .listener(ChunkDurationTrackerListener())
//            .build()
//    }

//    @Bean
//    fun accountReader(
//        accountRepository: SpringDataAccountRepository,
//    ): RepositoryItemReader<AccountEntity> {
//        return RepositoryItemReaderBuilder<AccountEntity>()
//            .name("accountReader")
//            .repository(accountRepository)
//            .methodName("findAllByDeletedAtIsNull")
//            .pageSize(chunkSize)
//            .sorts(mapOf("id" to Sort.Direction.ASC))
//            .build()
//    }

    @Bean
    fun noOffsetAccountReader(): NoOffsetAccountReader<AccountEntity> {
        val queryString =
            "SELECT acc " +
                    "FROM account acc " +
                    "WHERE acc.deletedAt is null " +
                    "ORDER BY acc.id ASC"
        return NoOffsetItemReaderBuilder<AccountEntity>()
            .entityManagerFactory(entityManagerFactory)
            .queryString(queryString)
            .parameterValues(emptyMap())
            .chunkSize(chunkSize)
            .name("noOffsetAccountReader")
            .idExtractor { it.id!! }
            .targetType(AccountEntity::class.java)
            .build()
    }

//    @Bean
//    fun jpaCursorAccountReader(): JpaCursorItemReader<AccountEntity> {
//        val queryString =
//            "SELECT acc " +
//                    "FROM account acc " +
//                    "WHERE acc.deletedAt is null " +
//                    "ORDER BY acc.id ASC"
//        return JpaCursorItemReaderBuilder<AccountEntity>()
//            .entityManagerFactory(entityManagerFactory)
//            .queryString(queryString)
//            .parameterValues(emptyMap())
//            .name("jpaCursorAccountReader")
//            .hintValues(Collections.singletonMap<String,Any>(HibernateHints.HINT_FETCH_SIZE, Int.MIN_VALUE))
//            .build()
//    }

    @Bean
    fun accountProcessor(): ItemProcessor<AccountEntity, Account> {
        return ItemProcessor { item ->
            item.toAccount()
        }
    }

    @Bean
    fun accountWriter(): ItemWriter<Account> {
        return ItemWriter { items ->
            println("items size: ${items.size()}")
            items.forEach {
                println("Account: $it")
            }
        }
    }
}