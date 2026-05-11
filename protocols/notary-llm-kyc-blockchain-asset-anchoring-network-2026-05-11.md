# Protocol Spec: Human Notary + LLM-Assisted KYC + Blockchain Asset Anchoring Network

Record ID: NOTARY-LLM-KYC-ANCHOR-NET-2026-05-11
Date: 2026-05-11
Primary anchor: overandor/gas-memory-collateral
Status: concept and architecture specification

## 1. Thesis

A human notary network can act as an official identity bridge for AI-assisted provenance systems.

The core pattern is:

human identity -> notary or authorized identity proofing -> signed declaration -> cryptographic hash -> blockchain or Git timestamp -> verifiable credential -> asset/provenance registry

The LLM does not replace the notary. The LLM prepares, extracts, classifies, validates structure, flags contradictions, assembles evidence packs, and helps route the workflow. The human notary or authorized proofing agent remains the identity authority.

## 2. Roles

### 2.1 Declarant

The human claiming ownership, authorship, observation, or control over an artifact, document, dataset, repository, field observation, or IP package.

### 2.2 Human Notary / Proofing Agent

The authorized human who confirms identity, witnesses the declaration, and completes the notarial or identity-proofing act under the rules of the relevant jurisdiction.

### 2.3 LLM Evidence Processor

The AI system that prepares the evidence packet, extracts document metadata, computes or records hashes, drafts declarations, detects missing fields, summarizes artifacts, and generates a provenance narrative.

### 2.4 Blockchain / Timestamp Layer

The public or permissioned ledger that anchors cryptographic hashes, timestamps, credential identifiers, revocation status, or registry pointers. The ledger should not store raw identity documents or private personal data.

### 2.5 Verifier

The party checking the provenance record, such as an investor, buyer, court, agency, marketplace, grant reviewer, counterparty, or registry operator.

## 3. Data Minimization Rule

Never put raw ID images, selfies, addresses, government ID numbers, signatures, biometric samples, private keys, API keys, or full KYC packets on-chain or in public GitHub repositories.

Public anchors should contain only:

- document hash
- timestamp
- artifact identifier
- issuer or notary identifier if public disclosure is appropriate
- credential status pointer
- optional redacted summary
- optional proof or signature reference

Private identity evidence should remain with the notary, qualified KYC provider, attorney, or compliant identity service provider.

## 4. Workflow

1. Declarant gathers artifact evidence.
2. LLM organizes the evidence packet.
3. LLM drafts a declaration or provenance statement.
4. Declarant reviews and corrects the statement.
5. Human notary or authorized proofing agent verifies identity.
6. Human notary completes the notarial act or identity-proofing event.
7. Final document is hashed with SHA-256 or stronger.
8. Hash is anchored in Git, OpenTimestamps, a blockchain transaction, or a registry smart contract.
9. A verifiable credential is issued to the declarant or artifact registry.
10. Verifiers check the hash, timestamp, notary/proofing record, credential status, and artifact chain.

## 5. Asset Types

This network can support:

- IP provenance records
- fieldwork observations
- AI-generated research artifacts
- software repository authorship packets
- invention disclosures
- dataset contribution claims
- notarized build logs
- timestamped product architecture
- municipal sanitation observations
- field evidence packs
- buyer or investor diligence packets

## 6. Trust Model

The LLM is not the root of trust.

Root trust comes from:

- identity proofing by a human notary or authorized provider
- cryptographic integrity through hashing and signatures
- public timestamping through Git or blockchain anchoring
- credential verification through standards-based credentials
- auditability through logs, exhibits, and revocation status

## 7. Verification Checklist

A verifier should be able to answer:

1. Who made the claim?
2. Who verified the person?
3. What artifact is being claimed?
4. What exact file or record was hashed?
5. Does the hash match the file presented now?
6. When was the hash anchored?
7. Is the credential still valid or revoked?
8. Is there any third-party license, fork, employment, or ownership issue?
9. Is the claim authorship, custody, observation, ownership, or valuation?
10. Is there a human accountable for the identity proofing event?

## 8. Legal Boundary

This protocol is not legal advice. Notarial validity, remote online notarization, KYC/AML obligations, data retention, privacy obligations, biometric handling, and credential admissibility vary by jurisdiction.

The network should use licensed counsel and compliance review before being marketed as a legal, financial, KYC, AML, securities, or identity-verification product.

## 9. Core Principle

LLM-assisted KYC can prepare and organize. Human notaries and authorized identity providers verify. Blockchain anchors integrity and time. Verifiable credentials make claims portable and machine-checkable.

The product is not fake certification. The product is a human-in-the-loop proof network.
