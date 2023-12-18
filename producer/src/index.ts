/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { z } from 'zod'
import { consumer } from './consumer'

export interface Env {
	TYPESENSE_PORT: 443
	TYPESENSE_HOST: string
	TYPESENSE_ADMIN_KEY: string
	// Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
	// MY_KV_NAMESPACE: KVNamespace;
	//
	// Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
	// MY_DURABLE_OBJECT: DurableObjectNamespace;
	//
	// Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
	// MY_BUCKET: R2Bucket;
	//
	// Example binding to a Service. Learn more at https://developers.cloudflare.com/workers/runtime-apis/service-bindings/
	// MY_SERVICE: Fetcher;
	//
	// Example binding to a Queue. Learn more at https://developers.cloudflare.com/queues/javascript-apis/
	MY_FIRST_QUEUE: Queue
}

const productSchema = z.object({
	collection_alias: z.string()
})

const insertOrUpdateProductSchema = productSchema.extend({
	odoo_id: z.number(),
	name: z.string(),
	display_name: z.string(),
	barcode: z.nullable(z.string()),
	sku: z.string(),
	description: z.nullable(z.string()),
	short_description: z.nullable(z.string()),
	deduplication_key: z.string(),
	url_key: z.string(),
	avatar_image: z.nullable(z.string()),
	images: z.array(z.string()),
	hierarchical_categories: z.object({
		lv0: z.string(),
		lv1: z.string(),
	}),
	uom_id: z.string(),
	brand: z.object({
		id: z.nullable(z.unknown()),
		name: z.nullable(z.string()),
		url_key: z.nullable(z.string()),
	}),
	price: z.object({
		value: z.number(),
		tax_included: z.boolean(),
		original_value: z.number(),
		discount: z.number(),
	}),
	variant_count: z.number(),
	variant_attributes: z.array(z.string()),
	dmi_ecommerce_product_backend_new_product: z.boolean(),
	dmi_ecommerce_tags: z.array(z.string()),
	dmi_product_sector: z.array(z.string()),
	dmi_ecommerce_packaging_ids: z.array(z.string()),
	dmi_ecommerce_optional_product_ids: z.array(z.string()),
	dmi_ecommerce_alternative_product_ids: z.array(z.string()),
	dmi_ecommerce_accesory_product_ids: z.array(z.string()),
	compatible_machines: z.array(z.string()),
})

const deleteProductSchema = productSchema.extend({
	odoo_id: z.number()
})

const requestSchema = z.discriminatedUnion('action', [
	z.object({
		action: z.enum(['create', 'update']),
		backend_id: z.number(),
		languages: z.array(z.enum(['en_US', 'es_ES'])),
		en_US: insertOrUpdateProductSchema.optional(),
		es_ES: insertOrUpdateProductSchema.optional(),
	}),
	z.object({
		action: z.literal('delete'),
		backend_id: z.number(),
		languages: z.array(z.enum(['en_US', 'es_ES'])),
		en_US: deleteProductSchema.optional().refine((data) => {
			console.log(data?.odoo_id)
			return true
		}),
		es_ES: deleteProductSchema.optional().refine((data) => {
			console.log(data?.odoo_id)
			return true
		}),
	})
])

const validateinsertOrUpdateProductSchema = (validatedData: z.infer<typeof requestSchema>) => {
	if (validatedData.languages.includes('en_US') && !validatedData.en_US) {
		throw new Error(`El campo en_US es obligatorio`)
	}

	if (!validatedData.languages.includes('en_US') && validatedData.en_US) {
		throw new Error(`El campo en_US no debería existir`)
	}

	if (validatedData.languages.includes('es_ES') && !validatedData.es_ES) {
		throw new Error(`El campo es_ES es obligatorio`)
	}

	if (!validatedData.languages.includes('es_ES') && validatedData.es_ES) {
		throw new Error(`El campo es_ES no debería existir`)
	}
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		// Lo validamos
		let validatedData
		try {
			// Obtenemos los datos de la query
			const data = await request.json()
			validatedData = requestSchema.parse(data)
		} catch (e) {
			const error = e as Error
			return new Response(`No se ha podido procesar el producto: ${error.message}`, {
				status: 400,
			})
		}

		// Ahora comprobamos la acción
		switch (validatedData.action) {
			case 'create' || 'update':
				return await insertOrUpdateProduct(validatedData, env)

			case 'delete':
				return await deleteProduct(validatedData, env)

			default:
				return new Response(`No se ha podido procesar el producto: Acción desconocida`, {
					status: 400,
				})
		}
	},
	async queue(batch: MessageBatch<any>, env: Env): Promise<void> {
		await consumer(batch, env)
	}
}

const insertOrUpdateProduct = async (validatedData: z.infer<typeof requestSchema>, env: Env) => {
	// Comprobamos que el campo languages concuerda con los campos recibidos
	try {
		validateinsertOrUpdateProductSchema(validatedData)
	} catch (e) {
		const error = e as Error
		return new Response(`No se ha podido procesar el producto: ${error.message}`, {
			status: 400,
		})
	}

	return await sendToQueue(validatedData, env)
}

const deleteProduct = async (validatedData: z.infer<typeof requestSchema>, env: Env) => {
	return await sendToQueue(validatedData, env)
}

const sendToQueue = async (validatedData: z.infer<typeof requestSchema>, env: Env) => {
	try {
		await env.MY_FIRST_QUEUE.send(validatedData)
	} catch (e) {
		const error = e as Error
		return new Response(`No se ha podido encolar el producto: ${error.message}`, {
			status: 500,
		})
	}

	return new Response('Producto encolado con éxito')
}


