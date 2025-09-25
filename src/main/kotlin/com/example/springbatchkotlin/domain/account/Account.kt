package com.example.springbatchkotlin.domain.account

import java.time.LocalDateTime

data class Account (
    val id: Long?,
    val thirdPartyAuthType: ThirdPartyAuthType,
    val thirdPartyAuthUid: String,
    val email: String,
    val languageCode: String,
    val countryCode: String,
    val timeZoneCode: String,
    val createdAt: LocalDateTime?,
    val updatedAt: LocalDateTime?,
    val deletedAt: LocalDateTime?
)
