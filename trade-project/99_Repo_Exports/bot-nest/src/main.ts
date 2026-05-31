/**
 * XAUUSD Order Flow Telegram Bot
 * 
 * Features:
 * - Sends signals from Redis notify:telegram stream to Telegram
 * - Handles inline button callbacks → writes to bot:callbacks stream
 * - Consumer group for at-most-once delivery
 */

import Redis from 'ioredis'
import 'reflect-metadata'
import { Markup, Telegraf } from 'telegraf'
import { IsString, IsOptional, IsDefined, validateSync, IsArray } from 'class-validator'
import { plainToInstance } from 'class-transformer'

// Configuration
const BOT_TOKEN = process.env.BOT_TOKEN || ''
const REDIS_URL = process.env.REDIS_URL || 'redis://scanner-redis:6379/0'
const CHAT_ID = process.env.CHAT_ID ? Number(process.env.CHAT_ID) : undefined
const GROUP = process.env.BOT_GROUP || 'bot-sender-group'
const CONSUMER = process.env.BOT_CONSUMER || 'bot-sender-1'
const NOTIFY_STREAM = process.env.NOTIFY_STREAM || 'notify:telegram'
const CALLBACKS_STREAM = process.env.CALLBACKS_STREAM || 'bot:callbacks'

// Connect to Redis
const r = new Redis(REDIS_URL)

// Create Telegram bot
const bot = new Telegraf(BOT_TOKEN)

export class NotifyPayloadDto {
	@IsOptional()
	@IsString()
	event_id?: string;

	@IsDefined()
	@IsString()
	text!: string;

	@IsOptional()
	@IsArray()
	buttons?: any[][]; 
}

/**
 * Ensure consumer group exists
 */
async function ensureGroup() {
	try {
		await r.xgroup('CREATE', NOTIFY_STREAM, GROUP, '0', 'MKSTREAM')
		console.log(`✅ Created consumer group: ${GROUP}`)
	} catch (e: any) {
		if (e.message && e.message.includes('BUSYGROUP')) {
			console.log(`✅ Consumer group already exists: ${GROUP}`)
		} else {
			console.error('Error creating group:', e)
		}
	}
}

/**
 * Sender loop - reads from Redis and sends to Telegram
 */
async function senderLoop() {
	await ensureGroup()

	console.log('📊 Listening for signals...')
	console.log(`   Stream: ${NOTIFY_STREAM}`)
	console.log(`   Group: ${GROUP}`)
	console.log(`   Consumer: ${CONSUMER}`)
	console.log(`   Chat ID: ${CHAT_ID || 'NOT SET'}`)
	console.log()

	let sentCount = 0

	while (true) {
		try {
			// Read from stream
			const res = await (r as any).xreadgroup(
				'GROUP', GROUP, CONSUMER,
				'BLOCK', 2000,
				'COUNT', 10,
				'STREAMS', NOTIFY_STREAM, '>'
			)

			if (!Array.isArray(res) || res.length === 0) {
				continue
			}

			for (const [_stream, entries] of res as Array<[string, Array<[string, string[]]>]>) {
				for (const [id, fields] of entries) {
					try {
						const obj: any = {};
						for (let i = 0; i < fields.length; i += 2) {
							obj[fields[i]] = fields[i + 1];
						}

						let parsedObj = obj;
						if (obj.payload) {
							try { parsedObj = JSON.parse(obj.payload); } catch(e) {}
						} else if (obj.data) {
							try { parsedObj = JSON.parse(obj.data); } catch(e) {}
						}

						if (parsedObj.buttons && typeof parsedObj.buttons === 'string') {
							try { parsedObj.buttons = JSON.parse(parsedObj.buttons); } catch(e) {}
						}

						const dto = plainToInstance(NotifyPayloadDto, parsedObj);
						const errors = validateSync(dto);
						if (errors.length > 0) {
							console.error(`❌ Validation failed for message ${id}:`, errors);
							continue;
						}

						// Dedup by event_id
						if (dto.event_id) {
							const dedupKey = `bot:processed:${dto.event_id}`;
							const isNew = await r.setnx(dedupKey, '1');
							if (!isNew) {
								console.log(`⏭️ Skipping duplicate event_id: ${dto.event_id}`);
								continue;
							}
							await r.expire(dedupKey, 86400); // 1 day TTL
						}

						// Build inline keyboard
						const markup = dto.buttons ? Markup.inlineKeyboard(
							dto.buttons.map(row => row.map((btn: any) =>
								Markup.button.callback(btn.text, btn.callback)
							))
						) : undefined

						// Send to Telegram
						if (!CHAT_ID) {
							console.log('⚠️  CHAT_ID not set, skipping send')
						} else {
							await bot.telegram.sendMessage(
								CHAT_ID,
								dto.text,
								markup ? markup : undefined
							)
							sentCount++
							console.log(`✅ Sent message #${sentCount}`)
						}

					} finally {
						// Always ACK
						await r.xack(NOTIFY_STREAM, GROUP, id)
					}
				}
			}
		} catch (err) {
			console.error('Error in sender loop:', err)
			await new Promise(resolve => setTimeout(resolve, 1000))
		}
	}
}

/**
 * Callback handler - writes callbacks to Redis stream
 */
bot.on('callback_query', async (ctx) => {
	try {
		const cb = ctx.callbackQuery
		const data = (cb as any).data as string

		// Write to Redis stream
		await r.xadd(
			CALLBACKS_STREAM,
			'*',
			'callback', data,
			'timestamp', Date.now().toString(),
			'chat_id', ctx.chat?.id.toString() || '',
			'user_id', ctx.from?.id.toString() || '',
			'username', ctx.from?.username || ''
		)

		console.log(`📥 Callback received: ${data}`)

		// Answer callback query
		await ctx.answerCbQuery('✅ Обработано')
	} catch (err) {
		console.error('Error handling callback:', err)
		await ctx.answerCbQuery('❌ Ошибка')
	}
})

/**
 * Start command
 */
bot.command('start', (ctx) => {
	ctx.reply(
		'🤖 XAUUSD Order Flow Bot v6.0\n\n' +
		'Бот для получения торговых сигналов по золоту.\n\n' +
		'Сигналы приходят автоматически с кнопками:\n' +
		'- Открыть - выполнить сделку\n' +
		'- SL/TP - установить уровни\n' +
		'- Отменить - пропустить сигнал\n' +
		'- x0.5/x1/x2 - изменить объем'
	)
})

/**
 * Status command
 */
bot.command('status', async (ctx) => {
	try {
		const queueLen = await r.llen('orders:queue')
		const execLen = await r.xlen('orders:exec')
		const notifyLen = await r.xlen(NOTIFY_STREAM)

		ctx.reply(
			'📊 Статус системы:\n\n' +
			`Signals stream: ${notifyLen}\n` +
			`Orders queue: ${queueLen}\n` +
			`Executions: ${execLen}`
		)
	} catch (err) {
		ctx.reply('❌ Ошибка получения статуса')
	}
})

// Launch bot
console.log('🤖 Starting Telegram bot...')
bot.launch()

// Start sender loop
senderLoop().catch(err => {
	console.error('Fatal error in sender loop:', err)
	process.exit(1)
})

// Graceful shutdown
process.once('SIGINT', () => {
	console.log('Received SIGINT, stopping...')
	bot.stop('SIGINT')
	r.disconnect()
})

process.once('SIGTERM', () => {
	console.log('Received SIGTERM, stopping...')
	bot.stop('SIGTERM')
	r.disconnect()
});
