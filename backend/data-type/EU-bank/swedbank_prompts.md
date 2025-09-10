# Swedbank Social Media Data Generation Prompts

## 1. Twitter Prompt

You are tasked with generating realistic posts about Swedbank consumer banking products and services. Create authentic content that reflects real customer experiences, complaints, or resolved issues.

**Instructions:**
- Generate a post based on the provided dominant_topic and subtopics
- Keep the text within 280-character limit
- Use authentic, conversational language that sounds like real customers
- Include relevant banking/financial hashtags (2-4 hashtags maximum)
- Content can range from customer complaints to resolved issues and positive experiences
- Make engagement metrics realistic (likes should be highest, followed by retweets, then replies, then quotes)
- Sentiment should align logically with the dominant topic and subtopics
- Priority should reflect business impact: P1 - Critical (urgent issues like security, money loss), P2 - Medium (service delays, feature problems), P3 - Low (minor inconveniences, positive feedback)
- Urgency = true for time-sensitive issues or negative experiences requiring immediate attention

**Context to consider:**
- Swedbank is a major Nordic bank
- Focus on consumer products: mobile banking, loans, cards, customer service, online banking
- Include @Swedbank mentions when appropriate for complaints or questions
- Content should reflect both unresolved complaints and successfully resolved customer issues

**Variables:**
Dominant_topic: "..."
Subtopics: "...", "...", "..."

**Output format:**
```json
{
  "hashtags": ["#tag1", "#tag2"],
  "priority": "<P1 - Critical / P2 - Medium / P3 - Low>",
  "like_count": <int>,
  "quote_count": <int>,
  "reply_count": <int>,
  "retweet_count": <int>,
  "sentiment": "<Positive/Negative/Neutral>",
  "text": "<post text within 280 chars>",
  "urgency": <true/false>
}
```

---

## 2. Reddit Prompt

You are generating posts for banking-related communities about Swedbank consumer experiences. Create detailed discussions that reflect real customer complaints and resolved banking issues.

**Instructions:**
- Create detailed, conversational posts based on the provided dominant_topic and subtopics
- Use storytelling format with context, problem description, and resolution status
- Length should be 2-5 sentences, detailed and community-oriented
- Include specific details that make the post feel authentic (timeframes, amounts, specific features)
- Use emojis meaningfully to enhance emotion or context (use sparingly, only when they add value to the message)
- Content should cover customer complaints, ongoing issues, and successfully resolved problems
- Make engagement metrics realistic for discussion platforms (comment_count should typically be highest, followed by like_count, then share_count)
- Priority and urgency should reflect business impact and time sensitivity
- Posts often seek community advice or share resolution experiences

**Context to consider:**
- Users typically provide background context and detailed explanations
- Content ranges from seeking advice on banking issues to sharing resolution success stories
- Complaints tend to be analytical with specific details
- Users often compare experiences and provide updates on issue resolution

**Variables:**
Dominant_topic: "..."
Subtopics: "...", "...", "..."

**Output format:**
```json
{
  "priority": "<P1 - Critical / P2 - Medium / P3 - Low>",
  "like_count": <int>,
  "share_count": <int>,
  "comment_count": <int>,
  "sentiment": "<Positive/Negative/Neutral>",
  "text": "<detailed post text>",
  "urgency": <true/false>
}
```

---

## 3. Trustpilot Prompt

You are creating reviews for Swedbank that reflect genuine customer experiences with their banking services and products. Generate content covering customer complaints and resolved service issues.

**Instructions:**
- Generate structured review content based on the provided dominant_topic and subtopics
- Rating should align with sentiment: 5 stars (very positive), 4 stars (positive), 3 stars (neutral/mixed), 2 stars (negative), 1 star (very negative)
- Use formal, review-appropriate language that sounds like genuine customer feedback
- Include specific details about the banking experience (timeframes, service quality, feature functionality)
- Content should reflect both negative experiences/complaints and positive resolutions
- Length should be 2-4 sentences, structured and informative
- Useful_count should be realistic (typically 0-50 for most reviews)
- Priority should reflect business impact on reputation and customer trust
- Reviews often include resolution outcomes and recommendations

**Context to consider:**
- Reviews are formal and detailed customer feedback
- Customers mention specific products (mobile app, loans, customer service, account management)
- Content includes both complaints about issues and praise for successful resolutions
- Reviews often include recommendations or warnings to other potential customers

**Variables:**
Dominant_topic: "..."
Subtopics: "...", "...", "..."

**Output format:**
```json
{
  "rating": <1-5>,
  "priority": "<P1 - Critical / P2 - Medium / P3 - Low>",
  "useful_count": <int>,
  "sentiment": "<Positive/Negative/Neutral>",
  "text": "<review text>",
  "urgency": <true/false>
}
```

---

## 4. App Store/Google Play Store Prompt

You are generating mobile app reviews for Swedbank's mobile banking application. Create authentic reviews that reflect customer complaints and resolved app issues.

**Instructions:**
- Create app-specific review content based on the provided dominant_topic and subtopics
- Decide whether this is an Apple App Store or Google Play Store review
- Rating should align with sentiment: 5 stars (excellent), 4 stars (good), 3 stars (average), 2 stars (poor), 1 star (terrible)
- Focus on mobile app functionality, performance, user experience, and customer service
- Content should range from complaints about app issues to positive feedback on resolved problems
- Length should be 1-3 sentences, concise but informative
- Include technical details relevant to mobile banking (login issues, crashes, feature problems, updates)
- Make engagement metrics realistic for app stores (like_count and comment_count should be proportional)
- Priority reflects impact on app performance and user experience

**Context to consider:**
- App store reviews focus specifically on mobile application experience
- Users comment on app performance, user interface, security, and functionality
- Reviews cover both technical complaints and successful issue resolutions
- Content includes feedback on app updates, new features, and customer support responsiveness
- Use @Swedbank mentions strategically when addressing complaints, questions, or thanking for resolutions (not in every review)

**Variables:**
Dominant_topic: "..."
Subtopics: "...", "...", "..."

**Output format:**
```json
{
  "rating": <1-5>,
  "priority": "<P1 - Critical / P2 - Medium / P3 - Low>",
  "like_count": <int>,
  "comment_count": <int>,
  "sentiment": "<Positive/Negative/Neutral>",
  "text": "<app review text>",
  "platform": "<App Store / Google Play Store>",
  "urgency": <true/false>
}
```