# MEMBRA QR Gateway — Solana Devnet Production Pattern

**Record ID:** MEMBRA-QR-GATEWAY-SOLANA-DEVNET-2026-05-11  
**System:** MEMBRA Human Chain / MCHAT / QR Artifact Support Gateway  
**Network discipline:** Devnet-first, mainnet only after legal/security review  
**Status:** Architecture and Anchor implementation pattern, not audited production code  

---

## 1. Core Boundary

The MEMBRA QR Gateway must not be a blind QR money machine.

Correct flow:

```text
QR scan
-> artifact support page
-> user reviews terms
-> user accepts terms
-> wallet prompts transaction
-> user signs transaction locally
-> SOL support payment sent
-> disclosed rebate returned
-> creator allocation sent
-> support receipt account created
-> proof event emitted
```

Hard boundaries:

```text
support payment != investment
rebate != yield
receipt != profit claim
QR != blind execution
manifest != token
token != guaranteed money
mainnet mint != legal clearance
```

---

## 2. Why Devnet First

Solana mainnet deployment should not begin with a public QR asking people to send money.

MEMBRA should use the normal Solana application path:

```bash
solana config set --url devnet
anchor build
anchor test --provider.cluster devnet
anchor deploy --provider.cluster devnet
```

Devnet validates:

- PDA derivation
- terms hash enforcement
- support transfer
- rebate calculation
- receipt account creation
- events
- frontend wallet signing
- indexer and dashboard state
- failure modes and pause controls

---

## 3. Repository Architecture

```text
membra-qr-gateway/
  Anchor.toml
  programs/
    membra_qr_gateway/
      src/
        lib.rs
        state.rs
        errors.rs
        events.rs
        instructions/
          initialize_artifact.rs
          support_artifact.rs
          update_artifact.rs
          pause_artifact.rs
          close_artifact.rs
  tests/
    membra_qr_gateway.ts
  app/
    qr-support-page/
```

---

## 4. Token / Payment Semantics

This program is not a security offering and not a promise of yield.

The receipt represents:

```text
proof_of_support
artifact_access
participation_provenance
accepted_terms_receipt
```

The receipt does not represent:

```text
investment_return
equity
revenue_share
claim_on_future_contributors
ownership_of_a_person
guaranteed_redemption
```

---

## 5. Anchor.toml Pattern

```toml
[features]
seeds = false
skip-lint = false

[programs.devnet]
membra_qr_gateway = "REPLACE_WITH_PROGRAM_ID"

[programs.localnet]
membra_qr_gateway = "REPLACE_WITH_PROGRAM_ID"

[registry]
url = "https://api.apr.dev"

[provider]
cluster = "devnet"
wallet = "~/.config/solana/id.json"

[scripts]
test = "yarn run ts-mocha -p ./tsconfig.json -t 1000000 tests/**/*.ts"
```

---

## 6. State Model

### Artifact Account

```rust
#[account]
pub struct Artifact {
    pub creator: Pubkey,
    pub artifact_id: String,
    pub proof_hash: [u8; 32],
    pub terms_hash: [u8; 32],
    pub terms_uri: String,
    pub total_support_lamports: u64,
    pub support_count: u64,
    pub max_support_lamports: u64,
    pub initial_rebate_bps: u16,
    pub floor_rebate_bps: u16,
    pub decay_per_support_bps: u16,
    pub paused: bool,
    pub created_at: i64,
    pub bump: u8,
    pub vault_bump: u8,
}
```

### Support Receipt Account

```rust
#[account]
pub struct SupportReceipt {
    pub artifact: Pubkey,
    pub supporter: Pubkey,
    pub creator: Pubkey,
    pub amount_lamports: u64,
    pub rebate_lamports: u64,
    pub creator_lamports: u64,
    pub rebate_bps: u16,
    pub accepted_terms_hash: [u8; 32],
    pub artifact_proof_hash: [u8; 32],
    pub client_reference_hash: [u8; 32],
    pub created_at: i64,
    pub receipt_index: u64,
    pub bump: u8,
}
```

---

## 7. Required Errors

```rust
#[error_code]
pub enum MembraError {
    #[msg("Invalid support amount.")]
    InvalidAmount,
    #[msg("Artifact is paused.")]
    ArtifactPaused,
    #[msg("Rebate basis points cannot exceed 50%.")]
    RebateTooHigh,
    #[msg("Invalid rebate curve.")]
    InvalidRebateCurve,
    #[msg("String too long.")]
    StringTooLong,
    #[msg("Math overflow.")]
    MathOverflow,
    #[msg("Accepted terms hash does not match artifact terms hash.")]
    TermsHashMismatch,
    #[msg("Support cap exceeded.")]
    SupportCapExceeded,
    #[msg("Only artifact creator can perform this action.")]
    UnauthorizedCreator,
}
```

---

## 8. Required Events

```rust
#[event]
pub struct ArtifactInitialized {
    pub artifact: Pubkey,
    pub creator: Pubkey,
    pub proof_hash: [u8; 32],
    pub terms_hash: [u8; 32],
    pub terms_uri: String,
    pub created_at: i64,
}

#[event]
pub struct SupportRecorded {
    pub artifact: Pubkey,
    pub supporter: Pubkey,
    pub creator: Pubkey,
    pub amount_lamports: u64,
    pub rebate_lamports: u64,
    pub creator_lamports: u64,
    pub rebate_bps: u16,
    pub receipt: Pubkey,
    pub receipt_index: u64,
    pub artifact_proof_hash: [u8; 32],
    pub accepted_terms_hash: [u8; 32],
    pub client_reference_hash: [u8; 32],
    pub created_at: i64,
}

#[event]
pub struct ArtifactStatusUpdated {
    pub artifact: Pubkey,
    pub creator: Pubkey,
    pub paused: bool,
    pub updated_at: i64,
}
```

---

## 9. QR Payload Pattern

The QR opens a web page. It does not execute directly.

```json
{
  "protocol": "MEMBRA-QR-0.1",
  "cluster": "devnet",
  "action": "OPEN_ARTIFACT_SUPPORT_PAGE",
  "artifact_id": "room-scan-1021",
  "program_id": "REPLACE_WITH_PROGRAM_ID",
  "artifact_pda": "ARTIFACT_PDA",
  "creator_public_wallet": "CREATOR_PUBLIC_WALLET",
  "terms_uri": "ipfs://TERMS_METADATA_HASH",
  "terms_hash": "sha256:TERMS_HASH",
  "proof_hash": "sha256:ARTIFACT_PROOF_HASH",
  "execution_requires_user_signature": true,
  "not_profit_guarantee": true,
  "not_investment": true
}
```

---

## 10. Rebate Curve

The disclosed rebate is calculated by support count:

```text
current_rebate_bps = max(
  initial_rebate_bps - decay_per_support_bps * support_count,
  floor_rebate_bps
)
```

Example:

```text
initial_rebate_bps = 5000   // 50%
floor_rebate_bps = 100      // 1%
decay_per_support_bps = 100 // -1% per support event
```

This is a disclosed rebate curve, not yield.

---

## 11. Important Implementation Correction

For production-quality Solana code, avoid unsafe direct lamport mutation from a generic unchecked vault unless ownership and signer semantics are fully correct.

Recommended safer approach:

1. Use a PDA vault account with clear ownership and signer seeds.
2. Prefer `system_program::transfer` / `invoke_signed` with vault signer seeds when moving lamports from PDA-controlled vaults.
3. Consider keeping support funds in the vault until explicit settlement instruction, rather than receiving and immediately routing in the same instruction.
4. Add tests proving creator allocation, rebate return, rent behavior, cap enforcement, pause handling, and malicious account substitution fail correctly.

The principle remains:

```text
supporter signs -> vault receives -> program verifies terms -> creator/rebate allocation routes -> receipt is created -> event emitted
```

---

## 12. Frontend Transaction Logic Pattern

```ts
export async function supportArtifact({
  program,
  supporter,
  creator,
  artifact,
  artifactVault,
  amountLamports,
  acceptedTermsText,
  clientReference,
}: {
  program: anchor.Program;
  supporter: PublicKey;
  creator: PublicKey;
  artifact: PublicKey;
  artifactVault: PublicKey;
  amountLamports: anchor.BN;
  acceptedTermsText: string;
  clientReference: string;
}) {
  const artifactAccount: any = await program.account.artifact.fetch(artifact);

  const acceptedTermsHash = Array.from(
    sha256(new TextEncoder().encode(acceptedTermsText))
  );

  const clientReferenceHash = Array.from(
    sha256(new TextEncoder().encode(clientReference))
  );

  const receiptIndex = artifactAccount.supportCount as anchor.BN;

  const [receipt] = PublicKey.findProgramAddressSync(
    [
      Buffer.from("receipt"),
      artifact.toBuffer(),
      supporter.toBuffer(),
      receiptIndex.toArrayLike(Buffer, "le", 8),
    ],
    program.programId
  );

  return await program.methods
    .supportArtifact(amountLamports, acceptedTermsHash, clientReferenceHash)
    .accounts({
      supporter,
      creator,
      artifact,
      artifactVault,
      receipt,
      systemProgram: SystemProgram.programId,
    })
    .rpc();
}
```

---

## 13. Devnet Launch State Machine

```text
DRAFT_CONTRACT
-> LOCAL_TEST_PASS
-> DEVNET_DEPLOYED
-> DEVNET_QR_GENERATED
-> TERMS_HASH_PUBLISHED
-> TEST_SUPPORT_PAYMENT_SENT
-> TEST_REBATE_RETURNED
-> TEST_RECEIPT_CREATED
-> EVENTS_INDEXED
-> SECURITY_REVIEW_REQUIRED
-> MAINNET_READY_PENDING_LEGAL
```

Dashboard state card:

```json
{
  "contract": "MEMBRA_QR_GATEWAY",
  "cluster": "devnet",
  "state": "DEVNET_DEPLOYED",
  "execution_requires_user_signature": true,
  "private_key_requested": false,
  "seed_phrase_requested": false,
  "profit_guarantee": false,
  "receipt_represents": [
    "proof_of_support",
    "artifact_access",
    "participation_provenance"
  ],
  "receipt_does_not_represent": [
    "investment_return",
    "equity",
    "revenue_share",
    "claim_on_future_contributors"
  ]
}
```

---

## 14. Mainnet Readiness Requirements

Before mainnet:

1. Independent smart-contract audit.
2. Legal review.
3. Terms of service.
4. Refund/rebate disclosure.
5. Tax handling.
6. Sanctions screening if applicable.
7. Rate limits.
8. Indexer.
9. Monitoring.
10. Pause controls.
11. Multisig upgrade authority.
12. Frontend warning screens.
13. Wallet-drain protection UX.
14. Authority policy publication.
15. Public proof capsule.

---

## 15. TikTok-Safe Explanation

```text
This is a devnet QR smart-contract test.
A scan opens terms, the user signs a support payment, the contract records proof, applies a disclosed rebate, and creates a participation receipt.
It is not a profit guarantee, not equity, and not a claim on future contributors.
```

---

## 16. MEMBRA Dashboard Component

Add component:

```text
Solana QR Gateway State Machine
```

Show:

- contract name
- cluster
- program ID
- artifact PDA
- terms hash
- proof hash
- current state
- support cap
- support count
- rebate bps
- receipt event count
- legal/security status
- mainnet readiness gate

Visual state:

```text
Draft -> Local Test -> Devnet Deployed -> QR Generated -> Terms Published -> Test Payment -> Rebate Returned -> Receipt Created -> Events Indexed -> Security Review -> Mainnet Pending Legal
```

---

## 17. Closing Doctrine

```text
QR opens context.
Wallet signs intent.
Program records proof.
Rebate is disclosed.
Receipt is provenance.
Support is not investment.
Mainnet waits for review.
```

**End of spec.**
