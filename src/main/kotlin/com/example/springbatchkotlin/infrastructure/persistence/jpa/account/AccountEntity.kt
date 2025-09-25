package com.example.springbatchkotlin.infrastructure.persistence.jpa.account

import com.example.springbatchkotlin.domain.account.Account
import com.example.springbatchkotlin.domain.account.ThirdPartyAuthType
import jakarta.persistence.*
import java.time.LocalDateTime

@Entity(name = "account")
data class AccountEntity (
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) val id: Long?,
    @Enumerated(EnumType.STRING) val thirdPartyAuthType: ThirdPartyAuthType,
    val thirdPartyAuthUid: String,
    val email: String,
    val languageCode: String,
    val countryCode: String,
    val timeZoneCode: String,
    val createdAt: LocalDateTime,
    val updatedAt: LocalDateTime?,
    val deletedAt: LocalDateTime?
) {

    constructor(account: Account, willDelete: Boolean? = null) :
            this(
                id = account.id,
                thirdPartyAuthType = account.thirdPartyAuthType,
                thirdPartyAuthUid = account.thirdPartyAuthUid,
                email = account.email,
                languageCode = account.languageCode,
                countryCode = account.countryCode,
                timeZoneCode = account.timeZoneCode,
                createdAt = if (account.id == null) {
                    LocalDateTime.now()
                } else {
                    account.createdAt ?: LocalDateTime.now()
                },
                updatedAt = if (account.id != null) {
                    if (willDelete == true) {
                        account.updatedAt
                    } else {
                        LocalDateTime.now()
                    }
                } else {
                    null
                },
                deletedAt = if (willDelete != null && willDelete == true) {
                    LocalDateTime.now()
                } else {
                    null
                }
            )

    fun toAccount(): Account = Account(
        id = this.id,
        thirdPartyAuthType = this.thirdPartyAuthType,
        thirdPartyAuthUid = this.thirdPartyAuthUid,
        email = this.email,
        languageCode = this.languageCode,
        countryCode = this.countryCode,
        timeZoneCode = this.timeZoneCode,
        createdAt = this.createdAt,
        updatedAt = this.updatedAt,
        deletedAt = this.deletedAt
    )
}
