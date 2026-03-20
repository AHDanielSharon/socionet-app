import OpenAI from 'openai';
import { config } from '@config/index';
import { db } from '@lib/db';
import { logger } from '@utils/logger';

let openai: OpenAI | null = null;

if (config.openai.apiKey) {
  openai = new OpenAI({ apiKey: config.openai.apiKey });
}

const SYSTEM_PROMPT = `You are the SOCIONET AI Assistant — an intelligent, privacy-first social media AI.

You help users with:
- Writing and improving posts, captions, and bios
- Content strategy, growth tactics, and audience insights
- Summarizing trending topics and feed activity
- Suggesting people, communities, and content to discover
- Drafting messages and creative content
- Analyzing post performance and engagement
- Generating hashtag strategies
- Video/reel script ideas
- Creative direction for photos and media

Tone: Helpful, concise, friendly, slightly witty. Use emojis sparingly.
Privacy: Never ask for or store sensitive personal information beyond the conversation.
Format: Use markdown for longer responses. Keep replies actionable.`;

const FALLBACK_RESPONSES = [
  "I'm your SOCIONET AI assistant! To unlock full AI capabilities including content generation, analytics, and smart suggestions, make sure `OPENAI_API_KEY` is set in your `.env` file. What would you like help with?",
  "Great question! For full AI-powered assistance, configure an OpenAI key in settings. I'm still here to help guide you around SOCIONET though!",
];

export const aiService = {
  chat: async (
    conversationId: string,
    userMessage: string,
    userId: string
  ): Promise<string> => {
    // Save user message
    await db.query(
      'INSERT INTO ai_messages (conversation_id, role, content) VALUES ($1, $2, $3)',
      [conversationId, 'user', userMessage]
    );

    // Get conversation history (last 20 messages)
    const history = await db.queryMany<{ role: string; content: string }>(
      `SELECT role, content FROM ai_messages
       WHERE conversation_id = $1
       ORDER BY created_at ASC LIMIT 20`,
      [conversationId]
    );

    let response: string;

    if (openai) {
      try {
        const completion = await openai.chat.completions.create({
          model: config.openai.model,
          messages: [
            { role: 'system', content: SYSTEM_PROMPT },
            ...history.map(m => ({
              role: m.role as 'user' | 'assistant',
              content: m.content,
            })),
          ],
          max_tokens: config.openai.maxTokens,
          temperature: config.openai.temperature,
          presence_penalty: 0.1,
          frequency_penalty: 0.1,
        });
        response = completion.choices[0]?.message?.content || 'I could not generate a response.';

        // Track token usage
        const tokens = completion.usage?.total_tokens || 0;
        await db.query(
          'UPDATE ai_conversations SET tokens_used = tokens_used + $1, updated_at = NOW() WHERE id = $2',
          [tokens, conversationId]
        );
      } catch (err) {
        logger.error('OpenAI error', { error: String(err) });
        response = 'I encountered an error. Please try again in a moment.';
      }
    } else {
      response = FALLBACK_RESPONSES[Math.floor(Math.random() * FALLBACK_RESPONSES.length)];
    }

    // Save assistant response
    await db.query(
      'INSERT INTO ai_messages (conversation_id, role, content) VALUES ($1, $2, $3)',
      [conversationId, 'assistant', response]
    );

    // Auto-generate conversation title from first exchange
    const msgCount = await db.queryOne<{ count: string }>(
      'SELECT COUNT(*) FROM ai_messages WHERE conversation_id = $1',
      [conversationId]
    );
    if (parseInt(msgCount?.count || '0') <= 2) {
      const title = userMessage.slice(0, 60) + (userMessage.length > 60 ? '…' : '');
      await db.query(
        'UPDATE ai_conversations SET title = $1 WHERE id = $2',
        [title, conversationId]
      );
    }

    return response;
  },

  quickTask: async (task: string, context: string): Promise<string> => {
    const prompts: Record<string, string> = {
      caption: `Write 3 different engaging social media captions for this content: "${context}"\n\nMake them varied:\n1. Professional/informative\n2. Casual/relatable\n3. Bold/attention-grabbing\n\nKeep each under 150 characters. Include 3-5 relevant hashtags for each.`,

      bio: `Write 3 compelling social media bio options for someone with this background: "${context}"\n\nEach should:\n- Be under 150 characters\n- Be memorable and authentic\n- Include personality\n- End with a hook or CTA`,

      hashtags: `Generate a strategic hashtag set for this content: "${context}"\n\nProvide:\n- 5 high-volume tags (1M+ posts)\n- 8 medium-volume tags (100K-1M posts)\n- 7 niche tags (10K-100K posts)\nFormat as #tag, one per line.`,

      reply: `Write 4 different reply options for this message: "${context}"\n\nVariations: Warm/friendly, Professional, Casual/funny, Thoughtful. Keep each under 100 chars.`,

      post_ideas: `Generate 8 unique content post ideas for this niche/context: "${context}"\n\nFor each idea include:\n- Post type (Photo/Video/Reel/Carousel/Text)\n- Hook (first line)\n- Brief concept (1 sentence)\n- Best time to post`,

      content_plan: `Create a 7-day content calendar for: "${context}"\n\nFor each day include: Day, Content type, Topic/angle, Key message, Format, Caption hook.`,

      analyze: `Analyze this social media content and provide insights: "${context}"\n\nCover: Strengths, Weaknesses, Engagement potential (1-10), Suggested improvements, Target audience, Best platforms.`,
    };

    if (!openai) {
      return `To use AI quick tasks, please configure OPENAI_API_KEY in your environment.\n\nTask requested: ${task}\nContext: ${context}`;
    }

    try {
      const completion = await openai.chat.completions.create({
        model: config.openai.model,
        messages: [
          { role: 'system', content: 'You are a social media expert AI. Be concise, creative, and actionable. Use markdown formatting.' },
          { role: 'user', content: prompts[task] || `Help with: ${task}\nContext: ${context}` },
        ],
        max_tokens: 600,
        temperature: 0.8,
      });

      return completion.choices[0]?.message?.content || 'Could not generate response.';
    } catch (err) {
      logger.error('OpenAI quick task error', { error: String(err) });
      throw new Error('AI service temporarily unavailable');
    }
  },

  createConversation: async (userId: string): Promise<string> => {
    const result = await db.queryOne<{ id: string }>(
      `INSERT INTO ai_conversations (user_id, title)
       VALUES ($1, 'New Conversation') RETURNING id`,
      [userId]
    );
    return result!.id;
  },

  isAvailable: (): boolean => openai !== null,
};
