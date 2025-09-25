package com.example.springbatchkotlin.configuration

import com.example.springbatchkotlin.domain.account.Account
import com.example.springbatchkotlin.infrastructure.persistence.jpa.account.AccountEntity
import com.example.springbatchkotlin.infrastructure.persistence.jpa.account.SpringDataAccountRepository
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.data.RepositoryItemReader
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.domain.Sort
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class AccountJobConfig(
    private val jobRepository: JobRepository
) {

    @Bean
    fun accountJob(accountStep: Step): Job {
        return JobBuilder("accountJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .start(accountStep)
            .build()
    }

    @Bean
    fun accountStep(
        transactionManager: PlatformTransactionManager,
        accountReader: ItemReader<AccountEntity>,
        accountProcessor: ItemProcessor<AccountEntity, Account>,
        accountWriter: ItemWriter<Account>
    ): Step {
        return StepBuilder("accountStep", jobRepository)
            .chunk<AccountEntity, Account>(100, transactionManager)
            .reader(accountReader)
            .processor(accountProcessor)
            .writer(accountWriter)
            .build()
    }

    @Bean
    fun accountReader(
        accountRepository: SpringDataAccountRepository
    ): RepositoryItemReader<AccountEntity> {
        return RepositoryItemReaderBuilder<AccountEntity>()
            .name("accountReader")
            .repository(accountRepository)
            .methodName("findAllByDeletedAtIsNull")
            .pageSize(100)
            .sorts(mapOf("id" to Sort.Direction.ASC))
            .build()
    }

    @Bean
    fun accountProcessor(): ItemProcessor<AccountEntity, Account> {
        return ItemProcessor { accountEntity ->
            accountEntity.toAccount()
        }
    }

    @Bean
    fun accountWriter(): ItemWriter<Account> {
        return ItemWriter { items ->
            items.forEach {
                println("Account: $it")
            }
        }
    }
}