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
	collection_alias: z.string(),
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

const requestSchema = z.object({
	backend_id: z.number(),
	languages: z.array(z.enum(['en_US', 'es_ES'])),
	en_US: productSchema.optional(),
	es_ES: productSchema.optional(),
})

// const requestAliasSchema = z.object({
// 	collection_name: z.string(),
// 	name: z.string()
// })

// const errorRequestAliasSchema = z.object({
// 	message: z.string()
// })

// const requestInserProductsSchema = z.array(
// 	z.object({
// 		success: z.boolean()
// 	})
// )

// const getCollectionFromAlias = async (
// 	alias: string,
// 	serverConfig: {
// 		apiKey: string
// 		host: string
// 	}
// ) => {
// 	let res
// 	try {
// 		res = await fetch(`https://${serverConfig.host}/aliases/${alias}`, {
// 			method: 'GET',
// 			headers: {
// 				'X-TYPESENSE-API-KEY': serverConfig.apiKey,
// 			},
// 		})
// 	} catch (e) {
// 		const error = e as Error
// 		throw new Error(`No se ha podido obtener la colección: ${error.message}`)
// 	}

// 	if (!res.ok) {
// 		throw new Error(`No se ha podido obtener la colección: ${res.statusText}`)
// 	}

// 	const data = await res.json()

// 	const validatedData = requestAliasSchema.safeParse(data)
// 	if (validatedData.success) {
// 		return validatedData.data.collection_name
// 	}

// 	const validatedErrorData = errorRequestAliasSchema.safeParse(data)
// 	if (validatedErrorData.success) {
// 		throw new Error(`No exista una colección para el alias: ${alias}}`)
// 	}
// }

// const insertProducts = async (
// 	alias: string,
// 	products: string,
// 	serverConfig: {
// 		apiKey: string
// 		host: string
// 	}
// ) => {
// 	let collection
// 	try {
// 		collection = await getCollectionFromAlias(alias, serverConfig)
// 	} catch (e) {
// 		const error = e as Error
// 		throw new Error(error.message)
// 	}

// 	try {
// 		const res = await fetch(`https://${serverConfig.host}/collections/${collection}/documents/import?action=create`, {
// 			method: 'POST',
// 			headers: {
// 				'Content-Type': 'text/plain',
// 				'X-TYPESENSE-API-KEY': serverConfig.apiKey,
// 			},
// 			body: products
// 		})
		
// 		if (!res.ok) {
// 			throw new Error(`No se ha podido insertar el producto: ${res.statusText}`)
// 		}
		
// 		const data = await res.text()
// 		const formattedData = JSON.parse('['.concat(data).concat(']').replace(/\n/g, ', '))

// 		const validatedRes = requestInserProductsSchema.safeParse(formattedData)
// 		if (!validatedRes.success) {
// 			throw new Error(`Respuesta inválida: ${res.statusText}`)
// 		}

// 		if (validatedRes.data.includes({ success: false })) {
// 			throw new Error(`No se ha podido insertar el producto: ${res.statusText}`)
// 		}
// 	} catch (e) {
// 		const error = e as Error
// 		throw new Error(`No se ha podido obtener la colección: ${error.message}`)
// 	}
// }

// const insertSpanishProducts = async (
// 	products: string,
// 	serverConfig: {
// 		apiKey: string
// 		host: string
// 	}
// ) => {
// 	const spanishProductsAlias = 'spanish-products-alias'
// 	await insertProducts(spanishProductsAlias, products, serverConfig)
// }

// const insertEnglishProducts = async (
// 	products: string,
// 	serverConfig: {
// 		apiKey: string
// 		host: string
// 	}
// ) => {
// 	const englishProductsAlias = 'english-products-alias'
// 	await insertProducts(englishProductsAlias, products, serverConfig)
// }

const validateProductSchema = (validatedData: z.infer<typeof requestSchema>) => {
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

// const parseProductsToJSONL = (products: z.infer<typeof productSchema>[]) => {
// 	let productsStr = ""
// 	let cont = 0
// 	for (const product of products) {
// 		if (cont !== 0) {
// 			productsStr += "\n"
// 		}

// 		productsStr += JSON.stringify(product)

// 		cont++
// 	}

// 	return productsStr
// }

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

		// Si ha pasado la validación
		// Comprobamos que el campo languages concuerda con los campos recibidos
		try {
			validateProductSchema(validatedData)
		} catch (e) {
			const error = e as Error
			return new Response(`No se ha podido procesar el producto: ${error.message}`, {
				status: 400,
			})
		}

		// Si se ha llegado hasta aquí enviamos a la cola
		try {
			await env.MY_FIRST_QUEUE.send(validatedData)
		} catch (e) {
			const error = e as Error
			return new Response(`No se ha podido encolar el producto: ${error.message}`, {
				status: 500,
			})
		}

		return new Response('Producto encolado con éxito')
	},
	// async queue(batch: MessageBatch<any>, env: Env): Promise<void> {
	// 	// Obtenemos todos los mensajes
	// 	const messages = batch.messages
	// 	// Sacamos todos los productos si cumplen con el esquema
	// 	const spanishProducts = []
	// 	const englishProducts = []
	// 	try {
	// 		for (const message of messages) {
	// 			const product = requestSchema.parse(message.body)

	// 			if (product.languages.includes('en_US')) {
	// 				const englishProduct = productSchema.parse(product.en_US)
	// 				englishProducts.push(englishProduct)
	// 			}
				
	// 			if (product.languages.includes('es_ES')) {
	// 				const spanishProduct = productSchema.parse(product.es_ES)
	// 				spanishProducts.push(spanishProduct)
	// 			}
	// 		}
			
	// 		// Insertamos los productos en typesense
	// 		if (spanishProducts.length > 0) {
	// 			const spanishProductsStr = parseProductsToJSONL(spanishProducts)
	// 			await insertSpanishProducts(spanishProductsStr, {
	// 				apiKey: env.TYPESENSE_ADMIN_KEY,
	// 				host: env.TYPESENSE_HOST
	// 			})
	// 		}
	
	// 		if (englishProducts.length > 0) {
	// 			const englishProductsStr = parseProductsToJSONL(englishProducts)
	// 			await insertEnglishProducts(englishProductsStr, {
	// 				apiKey: env.TYPESENSE_ADMIN_KEY,
	// 				host: env.TYPESENSE_HOST
	// 			})
	// 		}
	// 	} catch (e) {
	// 		const error = e as Error
	// 		console.error(`No se ha podido procesar el producto: ${error.message}`)
	// 	}

	// },
}
