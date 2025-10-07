#!/usr/bin/env python3
"""
Calculate Optimal Request Delay for OpenRouter
Based on RPM and TPM limits
"""

print("="*70)
print("OpenRouter Optimal Delay Calculator")
print("="*70)

# Known limits for :free models
rpm_limit = 20  # Requests Per Minute
tpm_limit_conservative = 40000  # Conservative estimate for Tokens Per Minute
tpm_limit_optimistic = 100000   # Optimistic estimate for Tokens Per Minute

# Your request characteristics
prompt_tokens = 4750  # Approximate prompt size
completion_tokens = 1500  # Approximate response size
total_tokens_per_request = prompt_tokens + completion_tokens

print("\n📊 Your Request Characteristics:")
print(f"   Prompt size: ~{prompt_tokens} tokens")
print(f"   Response size: ~{completion_tokens} tokens")
print(f"   Total per request: ~{total_tokens_per_request} tokens")

print("\n📊 OpenRouter Limits (for :free models):")
print(f"   RPM Limit: {rpm_limit} requests/minute")
print(f"   TPM Limit: Unknown (typically 40,000-100,000 for free tier)")

print("\n" + "="*70)
print("SCENARIO ANALYSIS")
print("="*70)

# Current settings
current_delay = 40.0
current_rpm = 60 / current_delay
current_tpm = current_rpm * total_tokens_per_request

print(f"\n1️⃣  CURRENT SETTINGS (BASE_REQUEST_DELAY = {current_delay}s):")
print(f"   Requests/minute: {current_rpm:.2f} RPM ({current_rpm/rpm_limit*100:.1f}% of RPM limit)")
print(f"   Tokens/minute: {current_tpm:.0f} TPM")
print(f"   Speed: {current_rpm * 60:.1f} requests/hour")
print(f"   Time for 600 chats: {(600 / (current_rpm * 60)):.1f} hours")

# Calculate optimal based on conservative TPM
safe_rpm_for_tpm_conservative = tpm_limit_conservative / total_tokens_per_request
safe_delay_tpm_conservative = 60 / safe_rpm_for_tpm_conservative

print(f"\n2️⃣  TPM-OPTIMIZED (Conservative TPM={tpm_limit_conservative}):")
print(f"   Safe RPM: {safe_rpm_for_tpm_conservative:.2f} RPM ({safe_rpm_for_tpm_conservative/rpm_limit*100:.1f}% of RPM limit)")
print(f"   Optimal delay: {safe_delay_tpm_conservative:.1f} seconds")
print(f"   Speed: {safe_rpm_for_tpm_conservative * 60:.1f} requests/hour")
print(f"   Time for 600 chats: {(600 / (safe_rpm_for_tpm_conservative * 60)):.1f} hours")
print(f"   ⚡ Speedup: {current_rpm / safe_rpm_for_tpm_conservative:.1f}x faster!")

# Calculate optimal based on optimistic TPM
safe_rpm_for_tpm_optimistic = tpm_limit_optimistic / total_tokens_per_request
safe_delay_tpm_optimistic = 60 / safe_rpm_for_tpm_optimistic

print(f"\n3️⃣  TPM-OPTIMIZED (Optimistic TPM={tpm_limit_optimistic}):")
print(f"   Safe RPM: {safe_rpm_for_tpm_optimistic:.2f} RPM ({safe_rpm_for_tpm_optimistic/rpm_limit*100:.1f}% of RPM limit)")
print(f"   Optimal delay: {safe_delay_tpm_optimistic:.1f} seconds")
print(f"   Speed: {safe_rpm_for_tpm_optimistic * 60:.1f} requests/hour")
print(f"   Time for 600 chats: {(600 / (safe_rpm_for_tpm_optimistic * 60)):.1f} hours")
print(f"   ⚡ Speedup: {current_rpm / safe_rpm_for_tpm_optimistic:.1f}x faster!")

# Calculate based on RPM limit only
rpm_safe_delay = (60 / rpm_limit) * 1.2  # Use 80% of max (20% buffer)
rpm_safe_rpm = 60 / rpm_safe_delay

print(f"\n4️⃣  RPM-OPTIMIZED (if no TPM limit):")
print(f"   Safe RPM: {rpm_safe_rpm:.2f} RPM (80% of max)")
print(f"   Optimal delay: {rpm_safe_delay:.1f} seconds")
print(f"   Speed: {rpm_safe_rpm * 60:.1f} requests/hour")
print(f"   Time for 600 chats: {(600 / (rpm_safe_rpm * 60)):.1f} hours")
print(f"   ⚡ Speedup: {current_rpm / rpm_safe_rpm:.1f}x faster!")

print("\n" + "="*70)
print("💡 RECOMMENDATIONS")
print("="*70)

print(f"\n🎯 RECOMMENDED DELAY: {safe_delay_tpm_conservative:.1f} seconds")
print(f"   This assumes conservative TPM limit of {tpm_limit_conservative}")
print(f"   Speed: {safe_rpm_for_tpm_conservative:.1f} requests/minute")
print(f"   Processing 600 chats: ~{(600 / (safe_rpm_for_tpm_conservative * 60)):.1f} hours")
print(f"   {safe_rpm_for_tpm_conservative / current_rpm:.1f}x faster than current!")

print(f"\n⚡ AGGRESSIVE OPTION: {safe_delay_tpm_optimistic:.1f} seconds")
print(f"   This assumes optimistic TPM limit of {tpm_limit_optimistic}")
print(f"   Speed: {safe_rpm_for_tpm_optimistic:.1f} requests/minute")
print(f"   Processing 600 chats: ~{(600 / (safe_rpm_for_tpm_optimistic * 60)):.1f} hours")
print(f"   {safe_rpm_for_tpm_optimistic / current_rpm:.1f}x faster than current!")

print("\n🔧 HOW TO UPDATE v2openrouter.py:")
print(f"   Change: BASE_REQUEST_DELAY = 40.0")
print(f"   To: BASE_REQUEST_DELAY = {safe_delay_tpm_conservative:.1f}  # Conservative, safe")
print(f"   OR: BASE_REQUEST_DELAY = {safe_delay_tpm_optimistic:.1f}  # Aggressive, faster")

print("\n⚠️  MONITORING:")
print("   Start with conservative setting")
print("   If NO rate limits after 50-100 requests → decrease delay further")
print("   If CONSTANT rate limits → increase delay")
print("   The script will auto-adapt with its built-in retry logic")

print("\n" + "="*70)

