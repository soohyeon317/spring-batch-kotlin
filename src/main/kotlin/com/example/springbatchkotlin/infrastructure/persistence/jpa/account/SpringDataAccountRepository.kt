package com.example.springbatchkotlin.infrastructure.persistence.jpa.account

import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository

interface SpringDataAccountRepository : JpaRepository<AccountEntity, Long> {

    fun findAllByDeletedAtIsNull(pageable: Pageable): Page<AccountEntity>
}
