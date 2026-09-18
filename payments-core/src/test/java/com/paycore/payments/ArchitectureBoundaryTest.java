package com.paycore.payments;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.paycore.payments.ledger.LedgerService;
import com.paycore.payments.ledger.LedgerTransaction;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * ArchUnit structural boundary tests (Section 5.7 & Section 7.16).
 *
 * <p>Enforces:
 * <ol>
 *   <li>Only the {@code ledger} package touches ledger classes/tables; external packages in {@code payments-core}
 *       may only call {@link LedgerService#post(LedgerTransaction)}.</li>
 *   <li>Services never import each other's packages.</li>
 *   <li>No JPA/Hibernate annotations exist on the money path (explicit SQL only).</li>
 * </ol>
 */
class ArchitectureBoundaryTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.paycore.payments");
    }

    @Test
    void onlyLedgerServicePostIsExposedOutsideLedgerPackage() {
        noClasses()
                .that()
                .resideOutsideOfPackage("..ledger..")
                .should()
                .callMethodWhere(com.tngtech.archunit.base.DescribedPredicate.describe(
                        "method in ..ledger.. other than LedgerService.post(LedgerTransaction)",
                        call -> {
                            String ownerPackage = call.getTargetOwner().getPackageName();
                            if (!ownerPackage.contains(".ledger")) {
                                return false;
                            }
                            if (call.getTargetOwner().isEquivalentTo(LedgerTransaction.class)
                                    || call.getTargetOwner().isEquivalentTo(com.paycore.payments.ledger.LedgerTransactionKind.class)) {
                                return false;
                            }
                            boolean isAllowedPost = call.getTargetOwner().isEquivalentTo(LedgerService.class)
                                    && call.getName().equals("post")
                                    && call.getTarget().getRawParameterTypes().size() == 1
                                    && call.getTarget().getRawParameterTypes().get(0).isEquivalentTo(LedgerTransaction.class);
                            return !isAllowedPost;
                        }))
                .check(classes);
    }

    @Test
    void paymentsCoreNeverImportsOtherServices() {
        noClasses()
                .that()
                .resideInAPackage("com.paycore.payments..")
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage(
                        "com.paycore.webhook..",
                        "com.paycore.readmodel..",
                        "com.paycore.reconciliation..",
                        "com.paycore.mockpsp..")
                .check(classes);
    }

    @Test
    void noJpaAnnotationsOnMoneyPath() {
        noClasses()
                .that()
                .resideInAPackage("com.paycore.payments..")
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage("jakarta.persistence..", "javax.persistence..", "org.hibernate..")
                .check(classes);
    }
}
