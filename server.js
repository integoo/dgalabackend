require('dotenv').config()
const express = require('express')
const app = express()
const { Client, Pool } = require('pg')
const bcrypt = require('bcrypt')
const jwt = require('jsonwebtoken') 
const bodyparser = require('body-parser')
const cors = require('cors')

const pool = new Pool({
	user: process.env.DB_USER,
	password: process.env.DB_PASSWORD,
	host: process.env.DB_HOST,
	database: process.env.DB_DATABASE,
	port: process.env.DB_PORT
})

//middleware
app.use(bodyparser.json())
app.use(bodyparser.urlencoded({
	extended:true
})
)


app.use(cors())
app.options('*',cors())




//routes
app.get('/HelloWorld', (req,res)=>{
	res.send('Hello World AWS Testing Server')
})


app.post('/login',async (req, res)=>{
	const user = req.body.user
	const password = req.body.password

	if (user === null || user === ''){
		//return res.status(401).send("Usuario No es Válido")
		return res.status(401).json({"error":"Usuario No es Válido"})
	}
	if (password === null || password === ''){
		//return res.status(401).send("Password No es Válido")
		return res.status(401).json({"error":"Password No es Válido"})
	}


	let response;
	let hashPassword;
	let values = [user]
	let sql = `SELECT "Password","ColaboradorId","SucursalId",'`+process.env.DB_DATABASE+`' AS db_name,"Administrador"
			FROM colaboradores 
			WHERE "User" = $1 UNION ALL SELECT '0','0','0','0','0'`
	try{
		response = await pool.query(sql, values)
	  	hashPassword = response.rows
	}catch(error){
		console.log(error.message)
		//return res.status(500).send(error.message)
		return res.status(500).json({"error": error.message})
	}


	if (hashPassword[0].Password === '0') {
		//return res.status(401).send("Usuario No Existe")
		return res.status(401).json({"error": "Usuario No Existe"})
	}	


	if(await bcrypt.compare(password, hashPassword[0].Password)) {
		//######################################################
		//jwt
		const jsonUser = { name: user }
		const accessToken = jwt.sign(jsonUser, process.env.ACCESS_TOKEN_SECRET, { expiresIn: '12h' })
		//######################################################


		//res.send('User :'+ user + ' Password : '+ password + ' hashPassword : '+ hashPassword[0].Password)
		//res.status(200).json({ "user": user, "accessToken": accessToken})
		res.status(200).json({ "error":'', "user": user, "accessToken": accessToken, "ColaboradorId": hashPassword[0].ColaboradorId, "SucursalId": hashPassword[0].SucursalId,"db_name": hashPassword[0].db_name,"Administrador":hashPassword[0].Administrador})
	}else{
		//res.status(401).send("Password Incorrecto") 
		res.status(401).json({"error":"Password Incorrecto"}) 
	}

})

app.get('/api/sucursales/:naturalezaCC',authenticationToken,async(req, res) => {
	/*let sql = `SELECT "SucursalId","Sucursal" AS "Sucursal","SucursalNombre","TipoSucursal" FROM sucursales 
		   WHERE "Status" = $1
		   ORDER BY "SucursalId"
	`
	*/

	let naturalezaCC = req.params.naturalezaCC;
	
	let sql = `SELECT DISTINCT s."SucursalId", s."Sucursal", "SucursalNombre", "TipoSucursal" FROM sucursales s
	        INNER JOIN catalogo_contable cc on cc."SucursalId" = s."SucursalId"
       		INNER JOIN cuentas_contables cco on cc."CuentaContableId" = cco."CuentaContableId" AND cco."Status" = $1 AND cco."NaturalezaCC"= $2
	WHERE s."Status" = $1
	ORDER BY s."SucursalId"
`


	let response
	const values = ['A',naturalezaCC]
	try{
		response = await pool.query(sql,values) 
		let data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/ingresos/unidadesdenegociocatalogo/:naturalezaCC',authenticationToken,async(req, res) => {
	let naturalezaCC = req.params.naturalezaCC;

	/*let sql = `SELECT DISTINCT cc."SucursalId", udn."UnidadDeNegocioId", udn."UnidadDeNegocio" 
			FROM unidades_de_negocio udn
			INNER JOIN catalogo_contable cc ON cc."UnidadDeNegocioId" = udn."UnidadDeNegocioId"
			ORDER BY udn."UnidadDeNegocioId"
	`
	*/

	let sql = `SELECT DISTINCT cc."SucursalId", udn."UnidadDeNegocioId", udn."UnidadDeNegocio" 
		FROM unidades_de_negocio udn
                INNER JOIN catalogo_contable cc on cc."UnidadDeNegocioId" = udn."UnidadDeNegocioId"
                INNER JOIN cuentas_contables cco on cc."CuentaContableId" = cco."CuentaContableId" AND cco."Status" = $1 AND cco."NaturalezaCC"= $2
        WHERE udn."Status" = $1
        ORDER BY udn."UnidadDeNegocioId"
`

	let values = ['A',naturalezaCC]
	let response
	try{
		response = await pool.query(sql, values)
		let data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/ingresos/unidadesdenegocio/:sucursal',authenticationToken,async(req, res) => {
	const vsucursal = req.params.sucursal
	let sql = `SELECT "UnidadDeNegocioId","UnidadDeNegocio" FROM unidades_de_negocio
			WHERE "Status"= $1 AND "UnidadDeNegocioId" IN (SELECT "UnidadDeNegocioId" FROM catalogo_contable 
									WHERE "SucursalId" = $2)
		`
	const values = ['A',vsucursal]
	let response
	try{
		response = await pool.query(sql,values)
		let data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
	
})

app.get('/ingresos/cuentascontablescatalogo/:naturalezaCC',authenticationToken, async(req, res) => {
	let naturalezaCC = req.params.naturalezaCC;

	/*let sql = `SELECT DISTINCT cac."SucursalId",cac."UnidadDeNegocioId",cc."CuentaContableId",cc."CuentaContable",cc."NaturalezaCC"
			FROM cuentas_contables cc
        		INNER JOIN catalogo_contable cac ON cac."CuentaContableId" = cc."CuentaContableId"
			ORDER BY cc."CuentaContableId"
	`
	*/



	let sql = `SELECT DISTINCT cac."SucursalId",cac."UnidadDeNegocioId", cc."CuentaContableId",cc."CuentaContable",cc."NaturalezaCC" 
                FROM  cuentas_contables cc 
                INNER JOIN catalogo_contable cac on cac."CuentaContableId" = cc."CuentaContableId"
        	WHERE cc."Status" = $1 AND cc."NaturalezaCC" = $2
        ORDER BY cc."CuentaContableId"
	`

	let response;
	const values = ['A',naturalezaCC]
	try{
		response = await pool.query(sql, values)
		let data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/ingresos/cuentascontables/:sucursal/:unidaddenegocio',authenticationToken, async(req, res) => {
	const vsucursal = req.params.sucursal
	const vunidaddenegocio = req.params.unidaddenegocio


	let sql =  `SELECT "CuentaContableId","CuentaContable" FROM cuentas_contables 
			WHERE "Status"= $1 AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM catalogo_contable
									WHERE "SucursalId" = $2 AND "UnidadDeNegocioId" = $3)
	`
	let response;
	const values =['A',vsucursal,vunidaddenegocio] 
 	try{
		response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/ingresos/subcuentascontablescatalogo/:naturalezaCC',authenticationToken, async(req,res) => {
	const naturalezaCC = req.params.naturalezaCC;

	/*let sql = `SELECT DISTINCT cc."SucursalId",cc."UnidadDeNegocioId",cc."CuentaContableId", cc."SubcuentaContableId", scc."SubcuentaContable"
			FROM subcuentas_contables scc
			INNER JOIN catalogo_contable cc ON cc."CuentaContableId" = scc."CuentaContableId" AND cc."SubcuentaContableId" = scc."SubcuentaContableId"
	`
	*/


	let sql = `SELECT DISTINCT cc."SucursalId",cc."UnidadDeNegocioId",scc."CuentaContableId", scc."SubcuentaContableId", scc."SubcuentaContable"
		FROM subcuentas_contables scc
		INNER JOIN cuentas_contables cco ON cco."CuentaContableId" = scc."CuentaContableId" AND cco."Status" = 'A'
                INNER JOIN catalogo_contable cc on cc."CuentaContableId" = scc."CuentaContableId" AND cc."SubcuentaContableId" = scc."SubcuentaContableId"
                WHERE scc."Status" = $1 AND cco."NaturalezaCC" = $2
        ORDER BY scc."SubcuentaContableId"
        `


	let response
	const values = ['A',naturalezaCC]
	try{
		response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error":error.message})
	}
})


app.get('/ingresos/subcuentascontables/:sucursal/:unidaddenegocio/:cuentacontable',authenticationToken, async(req, res) => {
	const vsucursal = req.params.sucursal
	const vunidaddenegocio = req.params.unidaddenegocio
	const vcuentacontable = req.params.cuentacontable

	let sql = `SELECT "SubcuentaContableId","SubcuentaContable" FROM subcuentas_contables
			WHERE "CuentaContableId" = $1
	`
	let response;
	const values = [vcuentacontable]
	try{
		response = await pool.query(sql, values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/ingresos/grabaingresos',authenticationToken, async(req, res) => {

	const vsucursalid = req.body.SucursalId
        const vunidaddenegocioid = req.body.UnidadDeNegocioId
        const vcuentacontableid = req.body.CuentaContableId
        const vsubcuentacontableid = req.body.SubcuentaContableId
        const vfecha = req.body.Fecha
        const vmonto = req.body.Monto
        const vcomentarios = req.body.Comentarios
	const vusuario = req.body.Usuario


	let values = []
	const client = await pool.connect();
	let sql = ''

	try{
		await client.query('BEGIN')
		values = [vsucursalid,vunidaddenegocioid,vcuentacontableid,vsubcuentacontableid,vcomentarios,vfecha,'202001',vmonto,'P',"now()",vusuario,'now()']
		sql = `INSERT INTO registro_contable VALUES (
		(SELECT COALESCE(MAX("FolioId"),0)+1 FROM registro_contable WHERE "SucursalId" = $1),
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7,
		$8,
		$9,
		$10,
		$11,
		$12)
		RETURNING "FolioId","SucursalId"
		`
		let response = await client.query(sql,values)
		//console.log(response.rows[0].SucursalId)
		await client.query('COMMIT')
		res.status(200).send("Success!!! FOLIO="+response.rows[0].FolioId)
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(400).send(error.message)
	}finally{
		client.release()
	}

})


app.get('/ingresos/getIngresos/:fecha/:sucursal',authenticationToken,async(req, res)=> {
	const vfecha = req.params.fecha
	const vsucursal = req.params.sucursal
	const vunidaddenegocio = req.params.unidaddenegocio
	const vcuentacontable = req.params.cuentacontable
	const vsubcuentacontable = req.params.subcuentacontable


	let sql = `SELECT rc."SucursalId",rc."FolioId",s."SucursalNombre",udn."UnidadDeNegocioId",udn."UnidadDeNegocioNombre",cc."CuentaContableId",
		cc."CuentaContable",scc."SubcuentaContableId",
		scc."SubcuentaContable",rc."Fecha",rc."Monto"
		FROM registro_contable rc
		INNER JOIN sucursales s ON rc."SucursalId"=s."SucursalId"
		INNER JOIN unidades_de_negocio udn ON rc."UnidadDeNegocioId"=udn."UnidadDeNegocioId"
		INNER JOIN cuentas_contables cc ON rc."CuentaContableId" = cc."CuentaContableId"
		INNER JOIN subcuentas_contables scc ON rc."CuentaContableId"= scc."CuentaContableId" AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		WHERE rc."Fecha" = $1 AND rc."SucursalId" = $2 
		`
		//WHERE rc."Fecha" = $1 AND rc."SucursalId" = $2 AND rc."UnidadDeNegocioId" = $3 AND rc."CuentaContableId" = $4 AND rc."SubcuentaContableId" = $5
	
	let response;
	//const values=[vsucursal,vfecha]
	//const values=[vfecha,vsucursal,vunidaddenegocio,vcuentacontable,vsubcuentacontable]
	const values=[vfecha,vsucursal]
	try{
		response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/periodoabierto',authenticationToken,async (req, res) => {
	let sql = `SELECT "Periodo","PrimerDiaMes" FROM cierres_mes
			WHERE "Status" = 'A'
	`
	let response;
	const values=[]
	try{
		const data = await pool.query(sql, values)
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/catalogos/:id',authenticationToken,async (req,res) => {
	const id = req.params.id
	let sql;
	if (id === '1'){
		sql = `SELECT "CategoriaId","Categoria" FROM categorias ORDER BY "CategoriaId"`
	}

	if (id === '2'){
		sql = `SELECT "CategoriaId","SubcategoriaId","Subcategoria" FROM subcategorias ORDER BY "CategoriaId", "SubcategoriaId"`
	}
	if (id === '3'){
		sql = `SELECT "MedidaCapacidadId","MedidaCapacidad" FROM medidas_capacidad ORDER BY "MedidaCapacidadId"`
	}
	if (id === '4'){
		sql = `SELECT "MedidaVentaId","MedidaVenta" FROM medidas_venta ORDER BY "MedidaVentaId"`
	}
	if (id === '5'){
		sql = `SELECT "MarcaId","Marca" FROM marcas ORDER BY "MarcaId"`
	}
	if ( id === '6'){
		sql = `SELECT "ColorId","Color" FROM colores ORDER BY "ColorId"`
	}
	if (id === '7'){
		sql = `SELECT "SaborId","Sabor" FROM sabores ORDER BY "SaborId"`
	}
	if (id === '8'){
		sql = `SELECT "IVAId","Descripcion", "IVA" FROM impuestos_iva ORDER BY "IVAId"`
	}
	if (id === '9'){
		sql = `SELECT "IEPSId","Descripcion", "IEPS" FROM impuestos_ieps ORDER BY "IEPSId"`
	}
	if (id === '10'){
		sql = `SELECT "SucursalId","Sucursal" FROM sucursales WHERE "Status" = 'A' AND "TipoSucursal" = 'S' ORDER BY "SucursalId"`
	}
	if (id === '11'){
		sql = `SELECT "ProveedorId","Proveedor","IVA" FROM proveedores WHERE "Status" = 'A' ORDER BY "ProveedorId"`
	}
	if (id === '12'){
		sql = `SELECT "SocioId","Socio" FROM socios WHERE "Status" = 'A'`
	}

	let response;
	try{
		response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.post('/api/altaProductos',authenticationToken,async(req,res)=>{
	const { CodigoBarras, Descripcion, CategoriaId, SubcategoriaId, UnidadesCapacidad, MedidaCapacidadId, UnidadesVenta, MedidaVentaId, MarcaId, ColorId,
	SaborId, IVAId, IVA, IVACompra, IEPSId, IEPS, Usuario } = req.body

	let vCodigoBarras=""
	const Maximo=6
	const Minimo=3



	const client = await pool.connect()
	try{
		await client.query('BEGIN')
		let sql = `SELECT COALESCE(MAX("CodigoId"),0)+1 AS "CodigoId" FROM productos`
		let response = await client.query(sql) 
		const CodigoId = response.rows[0].CodigoId

		if(CodigoBarras === 'I-CODE'){
			vCodigoBarras = 'I'+CodigoId.toString().padStart(12,0)
		}else{
			vCodigoBarras = CodigoBarras
		}

		let values=[CodigoId,vCodigoBarras, Descripcion, CategoriaId, SubcategoriaId, UnidadesCapacidad, MedidaCapacidadId, UnidadesVenta, MedidaVentaId, 
	    		    MarcaId, ColorId, SaborId, IVAId, IVACompra, IEPSId, Usuario]

		//INSERT productos
	     	sql = `INSERT INTO productos("CodigoId","CodigoBarras","Descripcion","CategoriaId","SubcategoriaId","UnidadesCapacidad","MedidaCapacidadId",
 	        	   "UnidadesVenta","MedidaVentaId","MarcaId","ColorId","SaborId","IVAId","IVACompra","IEPSId","Usuario","FechaHora") 
 			   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now()) RETURNING "CodigoId","CodigoBarras";
		`
		response = await client.query(sql,values)

		//INSERT codigos_barras
		values = [vCodigoBarras, CodigoId, Usuario]
		sql = `INSERT INTO codigos_barras ("CodigoBarras","CodigoId","FechaHora","Usuario") VALUES ($1,$2,NOW(),$3)`
		await client.query(sql,values)

		//INSERT inventario_perpetuo
		sql = `SELECT "SucursalId" FROM sucursales WHERE "TipoSucursal" = 'S'`
		const arregloSucursales = await client.query(sql)

		//arregloSucursales.rows.forEach(async (element)=>{
		for (let i=0; i < arregloSucursales.rows.length; i++){
			values=[arregloSucursales.rows[i].SucursalId,CodigoId,0,0,0,0.00,0.00,32,0.00,0.00,IVAId,IVA,0.00,IEPSId,IEPS,0.00,0.00,Maximo,Minimo,null,null,null,0.00,"now()",Usuario]

			sql = `INSERT INTO inventario_perpetuo (
			"SucursalId",
			"CodigoId",
			"UnidadesInventario",
			"UnidadesTransito",
			"UnidadesComprometidas",
			"CostoCompra",
			"CostoPromedio",
			"Margen",
			"MargenReal",
			"PrecioVentaSinImpuesto",
			"IVAId",
			"IVA",
			"IVAMonto",
			"IEPSId",
			"IEPS",
			"IEPSMonto",
			"PrecioVentaConImpuesto",
			"Maximo",
			"Minimo",
			"FechaCambioPrecio",
			"FechaUltimaVenta",
			"FechaUltimaCompra",
			"CostoBasePrecioVenta",
			"FechaHora",
			"Usuario")
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
			`
			await client.query(sql, values)
		}

		await client.query('COMMIT')
		res.status(200).json({"Success":response.rows[0].CodigoId+"  Codigo de Barras: "+vCodigoBarras})

	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error":error.message})
	}finally{
		client.release()
	}
})

app.post('/api/grabarecepcionordencompra',authenticationToken, async(req, res)=>{
	const { SucursalId, ProveedorId,IVAProveedor, NumeroFactura, TotalFactura, SocioId,Usuario, detalles } = req.body
	//console.log(detalles[0].CodigoId)


	const client = await pool.connect();
			let Secuencial = 0 
			let Status = 'R'
			let CategoriaId = 0 
			let SubcategoriaId = 0 
			let CodigoId = 0
			let CodigoBarras = "" 
			let UnidadesRecibidas = 0 
			let UnidadesInventario = 0
			let UnidadesInventarioAntes = 0 
			let UnidadesInventarioDespues= 0
			let CostoCompraSinImpuestos = 0.0
			let IVACostoCompra = 0.0
			let IEPS = 0
			let IEPSCostoCompra = 0.0
			let CostoCompra = 0.0
			let CostoCompraAnt = 0.0
			let CostoPromedioAnt = 0.0
			let Margen = 0.0
			let PrecioVentaSinImpuesto = 0.0 
			let PrecioVentaConImpuesto = 0.0
			let NuevoPrecioVentaSinImpuesto = 0.0 
			let NuevoPrecioVentaConImpuesto = 0.0
			let respuesta;

			let SocioPagoStatus = 'P'


			let CostoPromedio = 0.0

		let values=[]

		let MargenReal = 0.0
		let IVAId = 0
		let IVA = 0
		let IVAMonto = 0.0 
		let IEPSId = 0
		let IEPSMonto = 0.0
	try{
		await client.query('BEGIN')
		let sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM compras WHERE "SucursalId" = ${SucursalId}`
		let response = await client.query(sql) 
		let FolioId = response.rows[0].FolioId

		sql = `SELECT "ColaboradorId" FROM colaboradores WHERE "User" = '${Usuario}' ` 
		response = await client.query(sql)
		let ColaboradorId = response.rows[0].ColaboradorId


		for (let i=0; i < detalles.length; i++){

		sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",p."IVAId",ii."IVA",p."IEPSId",iie."IEPS",ip."CostoPromedio",ip."Margen",ip."PrecioVentaSinImpuesto",
			ip."PrecioVentaConImpuesto"
			FROM productos p
			INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = p."CodigoId"
			INNER JOIN impuestos_iva ii ON ii."IVAId" = p."IVAId"
			INNER JOIN impuestos_ieps iie ON iie."IEPSId" = p."IEPSId"
			WHERE ip."SucursalId" = ${SucursalId}  
			AND  p."CodigoId" = ${detalles[i].CodigoId} 
		`
			response = await client.query(sql)
			Secuencial = i + 1
			Status = 'R'
			CategoriaId = response.rows[0].CategoriaId
			SubcategoriaId = response.rows[0].SubcategoriaId
			CodigoId = detalles[i].CodigoId
			CodigoBarras = detalles[i].CodigoBarras
			UnidadesRecibidas = detalles[i].UnidadesRecibidas
			UnidadesInventario = response.rows[0].UnidadesInventario
			UnidadesInventarioAntes = response.rows[0].UnidadesInventario
			UnidadesInventarioDespues= parseInt(response.rows[0].UnidadesInventario) + parseInt(UnidadesRecibidas)
			CostoCompraSinImpuestos = detalles[i].CostoCompraSinImpuestos
			IVACostoCompra = detalles[i].IVAMonto
			IEPS = detalles[i].IEPS
			IEPSCostoCompra = detalles[i].IEPSMonto
			CostoCompra = detalles[i].CostoCompra
			CostoCompraAnt = response.rows[0].CostoCompra
			CostoPromedioAnt = response.rows[0].CostoPromedio
			Margen = response.rows[0].Margen
			IVAId = response.rows[0].IVAId
			IVAVenta = response.rows[0].IVA
			IEPSId = response.rows[0].IEPSId
			PrecioVentaSinImpuesto = response.rows[0].PrecioVentaSinImpuesto
			PrecioVentaConImpuesto = response.rows[0].PrecioVentaConImpuesto

			SocioPagoStatus = 'P'


			CostoPromedio = ((parseInt(UnidadesRecibidas) * parseFloat(CostoCompra)) + (parseInt(UnidadesInventario)*parseFloat(CostoCompraAnt))) / (parseInt(UnidadesRecibidas) + parseInt(UnidadesInventario))

			// Si SocioId es igual a uno es que la compra se realizó con dinero de la empresa y el SocioPagoStatus es CERRADO o COBRADO
			if (SocioId === 1){
				SocioPagoStatus = 'C'
			}

		values=[SucursalId,FolioId,Secuencial,Status,ProveedorId,NumeroFactura,TotalFactura,SocioId,SocioPagoStatus,"NOW()","NOW()",CategoriaId,SubcategoriaId,CodigoId,CodigoBarras,UnidadesRecibidas,UnidadesInventarioAntes,UnidadesInventarioDespues,IVAProveedor,IEPS,CostoCompraSinImpuestos,IVACostoCompra,IEPSCostoCompra,CostoCompra,CostoPromedio,CostoCompraAnt,CostoPromedioAnt,PrecioVentaSinImpuesto,PrecioVentaConImpuesto,ColaboradorId,"NOW()"]


		sql = `INSERT INTO compras 
		("SucursalId",
		"FolioId",
		"SerialId",
		"Status",
		"ProveedorId",
		"NumeroFactura",
		"TotalFactura",
		"SocioId",
		"SocioPagoStatus",
		"FechaOrden",
		"FechaRecepcion",
		"CategoriaId",
		"SubcategoriaId",
		"CodigoId",
		"CodigoBarras",
		"UnidadesRecibidas",
		"UnidadesInventarioAntes",
		"UnidadesInventarioDespues",
		"IVAProveedor",
		"IEPS",
		"CostoCompraSinImpuesto",
		"IVACostoCompra",
		"IEPSCostoCompra",
		"CostoCompra",
		"CostoPromedio",
		"CostoCompraAnt",
		"CostoPromedioAnt",
		"PrecioVentaSinImpuesto",
		"PrecioVentaConImpuesto",
		"ColaboradorId",
		"FechaHora")
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)
		RETURNING "FolioId"`


		respuesta = await client.query(sql, values)


			  if (PrecioVentaConImpuesto == 0){
				  
				//############### EN CASO DE PRECIO DE VENTA 0 (CERO) BUSCA SI EN OTRA SUCURSAL EL PRODUCTO TIENE PRECIO #####################
				values = [CodigoId]
				sql = `SELECT "PrecioVentaConImpuesto" 
					FROM inventario_perpetuo
					WHERE "CodigoId" = $1
					ORDER BY "PrecioVentaConImpuesto" DESC
					LIMIT 1
				`
				  response = await client.query(sql,values)


				  if(parseFloat(response.rows[0].PrecioVentaConImpuesto) > 0){
					NuevoPrecioVentaConImpuesto = parseFloat(response.rows[0].PrecioVentaConImpuesto)
					NuevoPrecioVentaSinImpuesto = NuevoPrecioVentaConImpuesto / (1+((IVAVenta+IEPS)/100)) 
					IVAMonto = (NuevoPrecioVentaSinImpuesto * (IVAVenta/100))
					IEPSMonto = (NuevoPrecioVentaSinImpuesto * (IEPS/100))
				  }else{
					NuevoPrecioVentaSinImpuesto = CostoPromedio / (1-(Margen/100))
					IVAMonto = (NuevoPrecioVentaSinImpuesto * (IVAVenta/100))
					IEPSMonto = (NuevoPrecioVentaSinImpuesto * (IEPS/100))
					NuevoPrecioVentaConImpuesto = NuevoPrecioVentaSinImpuesto + IVAMonto + IEPSMonto
					NuevoPrecioVentaConImpuesto = Math.ceil(NuevoPrecioVentaConImpuesto)

					//El siguiete código es para calcular el desglose de Margen Real e Impuestos a partir del Precio de Venta Redondeado a siguiente entero
					NuevoPrecioVentaSinImpuesto = (NuevoPrecioVentaConImpuesto / (1+((parseFloat(IVAVenta)+parseFloat(IEPS))/100))).toFixed(2)
					IVAMonto = (NuevoPrecioVentaSinImpuesto * (IVAVenta/100))
					IEPSMonto = (NuevoPrecioVentaSinImpuesto * (IEPS/100))
				  }


				MargenReal = (NuevoPrecioVentaSinImpuesto - CostoPromedio) / NuevoPrecioVentaSinImpuesto * 100


			values = [SucursalId, CodigoId, UnidadesRecibidas,CostoCompra,CostoPromedio,MargenReal,NuevoPrecioVentaSinImpuesto,IVAId,IVAVenta,IVAMonto,IEPSId,IEPS,IEPSMonto,NuevoPrecioVentaConImpuesto,Usuario]
			
				sql = `UPDATE inventario_perpetuo
					SET "UnidadesInventario" = "UnidadesInventario" + $3,
						"CostoCompra"= $4,
						"CostoPromedio" = $5,
						"MargenReal"= $6,
						"PrecioVentaSinImpuesto" = $7,
						"IVAId" = $8,
						"IVA" = $9,
						"IVAMonto" = $10,
						"IEPSId" = $11,
						"IEPS" = $12,
						"IEPSMonto" = $13,
						"PrecioVentaConImpuesto" = $14,
						"FechaCambioPrecio" = NOW(),
						"FechaUltimaCompra" = NOW(),
						"CostoBasePrecioVenta" = $5,
						"FechaHora" = NOW(),
						"Usuario" = $15
					WHERE "SucursalId" = $1 AND "CodigoId" = $2
				`

			}else{
			values = [SucursalId, CodigoId, UnidadesRecibidas,CostoCompra,CostoPromedio,Usuario]
				sql = `UPDATE inventario_perpetuo
					SET "UnidadesInventario" = "UnidadesInventario" + $3,
						"CostoCompra" = $4,
						"CostoPromedio" = $5,
						"MargenReal" = ("PrecioVentaSinImpuesto" - $5)/"PrecioVentaSinImpuesto" * 100,
						"FechaUltimaCompra" = NOW(),
						"FechaHora" = NOW(),
						"Usuario" = $6
					WHERE "SucursalId" = $1 AND "CodigoId" = $2
				`
			}
				await client.query(sql,values)
		}


		await client.query('COMMIT')
		//res.status(200).json({"Success": "SI"}) 
		res.status(200).json({"Success": "Folio : "+respuesta.rows[0].FolioId}) 
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})


function sqlventasinsert(){

	const sql = `INSERT INTO public.ventas ("SucursalId", "FolioId", "CodigoId", "SerialId", "Fecha", "FolioCompuesto", "Status", "ClienteId","CajeroId","VendedorId","CodigoBarras", "CategoriaId", "SubcategoriaId","FolioIdInventario","UnidadesRegistradas", "UnidadesVendidas", "UnidadesInventarioAntes", "UnidadesInventarioDespues", "CostoCompra", "CostoPromedio", "PrecioVentaSinImpuesto", "IVAId", "IVA", "IVAMonto", "IEPS", "IEPSMonto", "PrecioVentaConImpuesto", "UnidadesDevueltas","FechaDevolucionVenta","ComisionVentaPorcentaje","ComisionVenta","FechaHoraAlta","FechaHora","Usuario") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,$23, $24, $25, $26, $27, $28, $29, $30, $31, CLOCK_TIMESTAMP(), CLOCK_TIMESTAMP(),$32) RETURNING "FolioId"`

	return sql

}

app.post('/api/grabaventas',authenticationToken, async(req, res)=>{
	const { SucursalId, CajeroId, VendedorId, Usuario, detalles, Status, ClienteId } = req.body

	const client = await pool.connect();

	try{
		await client.query('BEGIN')
		let values=[SucursalId]
		let sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM ventas WHERE "SucursalId" = $1`
		let response = await client.query(sql,values) 
		const FolioId = response.rows[0].FolioId

		let FolioIdInventario = 0;

		if (Status === 'V'){
			values = [SucursalId]
			sql = `SELECT COALESCE(MAX("FolioIdInventario"),0)+1 AS "FolioIdInventario" FROM ventas WHERE "SucursalId" = $1`
			response = await client.query(sql,values) 
			FolioIdInventario = response.rows[0].FolioIdInventario
		}else{
			FolioIdInventario = 0;
		}

		const FolioCompuesto = SucursalId.toString().padStart(3, "0")+FolioId.toString().padStart(7, "0") 

		let CodigoId = 0
		let SerialId = 0;
		let Fecha="";
		let CodigoBarras;
		let CategoriaId;
		let SubcategoriaId;
		let UnidadesVendidas;
		let UnidadesInventarioAntes;
		let UnidadesInventarioDespues;
		let CostoCompra;
		let CostoPromedio;
		let PrecioVentaSinImpuesto;
		let Inventariable;
		let IVAId;
		let IVA;
		let IVAMonto;
		let IEPS;
		let IEPSMonto;
		let PrecioVentaConImpuesto;
		let FechaHora;
		let UnidadesDevueltas;
		let FechaDevolucionVenta;
		let ComisionVentaPorcentaje;
		let ComisionVenta;
		let respuesta;
		let UnidadesRegistradas = 0;


		for (let i=0; i < detalles.length; i++){

			CodigoId = parseInt(detalles[i].CodigoId)

			values = [SucursalId, CodigoId]
			sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",p."IVAId",ii."IVA",p."IEPSId",iie."IEPS",
				ip."CostoPromedio",ip."Margen",ip."PrecioVentaSinImpuesto",p."Inventariable"
				FROM productos p
				INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = p."CodigoId"
				INNER JOIN impuestos_iva ii ON ii."IVAId" = p."IVAId"
				INNER JOIN impuestos_ieps iie ON iie."IEPSId" = p."IEPSId"
				WHERE ip."SucursalId" = $1  
				AND  p."CodigoId" = $2 
				`
			response = await client.query(sql,values)

			CodigoBarras = detalles[i].CodigoBarras
			CategoriaId = response.rows[0].CategoriaId
			SubcategoriaId = response.rows[0].SubcategoriaId
			UnidadesRegistradas= detalles[i].Unidades

			if (Status === 'V'){ //Venta ya afectando Inventario
				SerialId = parseInt(detalles[i].SerialId) 
				Fecha = 'NOW()' //CAMBIARLO POR CURRENT_DATE o formando la fecha con un "new Date()" de nodejs
				UnidadesVendidas= detalles[i].Unidades;
				UnidadesInventarioAntes = response.rows[0].UnidadesInventario
				UnidadesInventarioDespues= parseInt(response.rows[0].UnidadesInventario) - parseInt(UnidadesVendidas)
			}else{
				SerialId = i+1 
				Fecha = null 
				UnidadesVendidas= 0;
				UnidadesInventarioAntes = 0 
				UnidadesInventarioDespues = 0 
			}

			CostoCompra = response.rows[0].CostoCompra
			CostoPromedio = response.rows[0].CostoPromedio




			Inventariable = response.rows[0].Inventariable
			IVAId = response.rows[0].IVAId
			IVA = response.rows[0].IVA
			IEPS = response.rows[0].IEPS


 			// CodigoId 65 es el "SERVICIO POR ENCARGO" de lavamatica y se calcula su Precio en base a un % sobre la venta de Productos de CategoriaId = 3
			if(CodigoId === 65){
				PrecioVentaSinImpuesto = parseFloat(detalles[i].PrecioVentaConImpuesto) /(1+((parseFloat(IVA) + parseFloat(IEPS))/100))
			}else{
				PrecioVentaSinImpuesto = response.rows[0].PrecioVentaSinImpuesto
			}

			IVAMonto = parseFloat(PrecioVentaSinImpuesto) * parseFloat(IVA/100) 
			IEPSMonto = parseFloat(PrecioVentaSinImpuesto) * parseFloat(IEPS/100) 
			PrecioVentaConImpuesto = detalles[i].PrecioVentaConImpuesto
			//FechaHora= 'NOW()'
			UnidadesDevueltas = 0
			FechaDevolucionVenta = null
			ComisionVentaPorcentaje = 0
			ComisionVenta = 0



			values = [SucursalId, FolioId, CodigoId, SerialId, Fecha, FolioCompuesto, Status, ClienteId, CajeroId, VendedorId, CodigoBarras, CategoriaId, SubcategoriaId, FolioIdInventario, UnidadesRegistradas, UnidadesVendidas,UnidadesInventarioAntes, UnidadesInventarioDespues, CostoCompra,CostoPromedio,PrecioVentaSinImpuesto,IVAId,IVA,IVAMonto,IEPS,IEPSMonto,PrecioVentaConImpuesto,UnidadesDevueltas, FechaDevolucionVenta, ComisionVentaPorcentaje, ComisionVenta,Usuario]


			sql = sqlventasinsert()  //Manda llamar el query sql para insertar a la tabla de ventas

			respuesta = await client.query(sql, values)

		if (Status === 'V' && Inventariable === 'S'){
			values = [SucursalId, CodigoId, UnidadesVendidas,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesInventario" = "UnidadesInventario" - $3,
					"FechaUltimaVenta" = CURRENT_DATE,
					"FechaHora" = CLOCK_TIMESTAMP(),
					"Usuario" = $4
				WHERE "SucursalId" = $1 AND "CodigoId" = $2
			`

				await client.query(sql,values)
		}
	}
		await client.query('COMMIT')

		res.status(200).json({"Success": respuesta.rows[0].FolioId}) 
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})


app.put('/api/eliminaregistroventapendiente',authenticationToken, async(req, res) =>{
	const SucursalId = req.body.SucursalId
	const FolioId = req.body.FolioId
	const CodigoId = req.body.CodigoId
	const SerialId = req.body.SerialId
	const Usuario = req.body.Usuario
	const ColaboradorId = req.body.ColaboradorId

	const client = await pool.connect()

	try{
		await client.query('BEGIN')
		const values = [SucursalId,FolioId,CodigoId,SerialId,Usuario,ColaboradorId]
		const sql = `UPDATE ventas
				SET "Status" = 'C',
					"FechaHora" = CLOCK_TIMESTAMP(),
					"Usuario" = $5,
					"VendedorId" = $6,
					"CajeroId" = $6
				WHERE "SucursalId" = $1
				AND "FolioId" = $2
				AND "CodigoId" = $3
				AND "SerialId" = $4
		`
		const response = await client.query(sql,values)

		await client.query('COMMIT')

		res.status(200).json({"message": "Success!!!"})

	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})


app.post('/api/agregaregistroventapendiente',authenticationToken,async(req, res)=>{
	const { SucursalId, FolioId, ClienteId, CajeroId, VendedorId, SerialId, CodigoId, CodigoBarras, UnidadesRegistradas, PrecioVentaConImpuesto, Usuario } = req.body

	const client = await pool.connect()

	try{
		await client.query('BEGIN')


		let values = [SucursalId,FolioId]
		let sql = `SELECT MAX("SerialId")+1 AS "SerialId", MAX("FolioCompuesto") AS "FolioCompuesto"
			FROM ventas  
			WHERE "SucursalId"= $1 AND "FolioId" = $2`

		let response = await client.query(sql,values)
		const SerialId = response.rows[0].SerialId
		const FolioCompuesto = response.rows[0].FolioCompuesto


		values = [SucursalId,CodigoId]
		sql = `SELECT p."CategoriaId",p."SubcategoriaId",ip."CostoCompra",ip."CostoPromedio",ip."PrecioVentaSinImpuesto",
			ip."IVAId",ip."IVA",ip."IVAMonto",ip."IEPS",ip."IEPSMonto"
			FROM productos p INNER JOIN inventario_perpetuo ip ON p."CodigoId" = ip."CodigoId"
			WHERE ip."SucursalId"= $1 AND p."CodigoId" = $2`

		response = await client.query(sql,values)


		const CategoriaId = response.rows[0].CategoriaId
		const SubcategoriaId = response.rows[0].SubcategoriaId
		const CostoCompra = parseFloat(response.rows[0].CostoCompra)
		const CostoPromedio = parseFloat(response.rows[0].CostoPromedio)
		const PrecioVentaSinImpuesto = parseFloat(response.rows[0].PrecioVentaSinImpuesto)
		const IVAId = response.rows[0].IVAId
		const IVA = parseFloat(response.rows[0].IVA)
		const IVAMonto = parseFloat(response.rows[0].IVAMonto)
		const IEPS = parseInt(response.rows[0].IEPS)
		const IEPSMonto = parseFloat(response.rows[0].IEPSMonto)
		

		const Fecha = null
		const Status = 'P'
		const UnidadesVendidas = 0


		sql = sqlventasinsert()


		values = [SucursalId, FolioId, CodigoId, SerialId,Fecha,FolioCompuesto,Status,ClienteId,CajeroId,VendedorId,CodigoBarras,CategoriaId,SubcategoriaId,
			0,
			UnidadesRegistradas,
			UnidadesVendidas,
			0, 
			0, 
			CostoCompra,CostoPromedio,PrecioVentaSinImpuesto,IVAId,IVA,IVAMonto,IEPS,IEPSMonto,PrecioVentaConImpuesto,
			0,
			Fecha,
			0,
			0,
			Usuario]
			


		response = await client.query(sql, values)
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})


app.put('/api/cierraventa',authenticationToken, async(req,res) => {
	const { SucursalId, FolioId, CajeroId, VendedorId, Usuario } = req.body


	let SerialId = 0
	let CodigoId = 0
	let UnidadesRegistradas = 0
	let sql;
	let response;
	let data;
	let values;
	let UnidadesInventarioAntes = 0
	let UnidadesInventarioDespues = 0
	let FolioInventario = 0
	let Inventariable;

	const client = await pool.connect()

	try{

		values = [SucursalId]
		sql = `SELECT COALESCE(MAX("FolioIdInventario"),0)+1 AS "FolioIdInventario" FROM ventas WHERE "SucursalId" = $1`
		response = await client.query(sql,values)
		const FolioIdInventario = response.rows[0].FolioIdInventario

		values = [SucursalId,FolioId]
		sql=`SELECT "CodigoId","SerialId","UnidadesRegistradas" FROM ventas
			WHERE "SucursalId" = $1
			AND "FolioId" = $2
			AND "Status" = 'P'
		`
		response = await client.query(sql,values)

		let arreglo = response.rows

		await client.query('BEGIN')

		for(let i=0; i<arreglo.length; i++){
			SerialId = parseInt(arreglo[i].SerialId)
			CodigoId = parseInt(arreglo[i].CodigoId)
			UnidadesRegistradas = parseInt(arreglo[i].UnidadesRegistradas)

			values = [SucursalId, CodigoId]
			sql = `SELECT ip."UnidadesInventario", p."Inventariable"
			FROM inventario_perpetuo ip INNER JOIN productos p ON ip."CodigoId" = p."CodigoId"
			WHERE ip."SucursalId" = $1 AND ip."CodigoId" = $2`
			response = await client.query(sql,values)
			UnidadesInventarioAntes = response.rows[0].UnidadesInventario
			Inventariable = response.rows[0].Inventariable

			UnidadesInventarioDespues = parseInt(UnidadesInventarioAntes) - parseInt(UnidadesRegistradas)

			values = [SucursalId,FolioId,CodigoId,SerialId,UnidadesRegistradas,UnidadesInventarioAntes,UnidadesInventarioDespues,parseInt(CajeroId),parseInt(VendedorId),Usuario,FolioIdInventario]

			sql = `UPDATE ventas
				SET
				"Status" = 'V',
				"UnidadesVendidas" = $5,
				"UnidadesInventarioAntes" = $6,
				"UnidadesInventarioDespues" = $7,
				"CajeroId"= $8,
				"VendedorId" = $9,
				"Fecha" = CURRENT_DATE,
				"FechaHora" = CLOCK_TIMESTAMP() ,
				"Usuario" = $10,
				"FolioIdInventario" = $11
			WHERE "SucursalId" = $1 AND "FolioId" = $2 AND "CodigoId" = $3 AND "SerialId" = $4
			`
			await client.query(sql,values)


			if(Inventariable === 'S'){
				values = [SucursalId, CodigoId, UnidadesInventarioDespues,Usuario]
				sql = `UPDATE inventario_perpetuo
					SET "UnidadesInventario" = $3,
					"FechaHora" = CLOCK_TIMESTAMP(),
					"Usuario" = $4
					WHERE "SucursalId" = $1 AND "CodigoId" = $2
				`
				await client.query(sql,values)
			}
		}
			await client.query('COMMIT')
			res.status(200).json({"message": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}

})


app.get('/api/consultaProductosRecientes',authenticationToken, async(req, res)=>{

	let sql = `SELECT "CodigoId","CodigoBarras","Descripcion" FROM vw_productos_descripcion ORDER BY "CodigoId" DESC LIMIT 10`

	try{
		const response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})

	}
})

app.get('/api/productodescripcion/:id',authenticationToken, async(req, res) =>{
	const id = req.params.id
	const sql = `SELECT vwpd."CodigoId", vwpd."Descripcion",p."IVAId",ii."Descripcion" AS "IVADescripcion",ii."IVA",
			p."IEPSId",ie."Descripcion" AS "IEPSDescripcion", ie."IEPS",p."IVACompra",vwpd."CompraVenta"
			FROM vw_productos_descripcion vwpd
			INNER JOIN productos p ON p."CodigoId" = vwpd."CodigoId"
			INNER JOIN impuestos_iva ii ON ii."IVAId" = p."IVAId"
			INNER JOIN impuestos_ieps ie ON ie."IEPSId" = p."IEPSId"
			INNER JOIN codigos_barras cb ON vwpd."CodigoId" = cb."CodigoId"
			WHERE cb."CodigoBarras" = $1`
	const values=[id]
	try{
		const response = await pool.query(sql, values)
		let data;
		if(response.rowCount === 0){
			return res.status(200).json({"error": "Producto No Existe"})

		}else{
			data = response.rows
		}
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/productosdatosventa/:SucursalId/:id',authenticationToken,async (req,res) => {
	const codbar = req.params.id
	const suc = req.params.SucursalId
	const sql = `SELECT vw."CodigoId",vw."Descripcion",ip."PrecioVentaConImpuesto",ip."CostoPromedio",vw."Inventariable",vw."CompraVenta",vw."CategoriaId" 
			FROM vw_productos_descripcion vw
			INNER JOIN codigos_barras cb ON cb."CodigoId" = vw."CodigoId"
			INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = vw."CodigoId"
			WHERE ip."SucursalId" = $1
			AND cb."CodigoBarras" = $2
			AND vw."CompraVenta" IN ('A','V') 
	`
	const values = [suc,codbar]
	try{
		const response = await pool.query(sql, values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}

})

app.get('/api/productosdescripcion/:desc',authenticationToken,async(req,res) => {
	const desc = '%'+req.params.desc+'%'
	const sql = `SELECT vw."CodigoId",vw."CodigoBarras",vw."Descripcion" FROM vw_productos_descripcion vw
			WHERE vw."Descripcion" LIKE $1 
			AND vw."CompraVenta" IN ('A','V')
	`
	const values = [desc]
	let data = []
	try{
		const response = await pool.query(sql, values)
		if(response.rowCount > 0){
			data = response.rows
		}
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/productodescripcionporcodigobarras/:SucursalId/:CodigoBarras/:SoloInventariable',authenticationToken,async(req,res) => {
	const SucursalId = parseInt(req.params.SucursalId)
	const CodigoBarras = req.params.CodigoBarras
	const SoloInventariable = req.params.SoloInventariable
	let sql=""
	let values = []

	try{
		values = [SucursalId,CodigoBarras]
		if(SoloInventariable === 'S'){
			sql = `SELECT vw."Descripcion",ip."UnidadesInventario",ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles",
				vw."CodigoId"
				FROM vw_productos_descripcion vw 
				INNER JOIN codigos_barras cb ON vw."CodigoId" = cb."CodigoId"
				INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = cb."CodigoId"
				WHERE ip."SucursalId" = $1
				AND cb."CodigoBarras" = $2
				AND vw."Inventariable" = 'S'
				`

		}else{
			sql = `SELECT vw."Descripcion",ip."UnidadesInventario",ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles",
				vw."CodigoId"
				FROM vw_productos_descripcion vw 
				INNER JOIN codigos_barras cb ON vw."CodigoId" = cb."CodigoId"
				INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = cb."CodigoId"
				WHERE ip."SucursalId" = $1
				AND cb."CodigoBarras" = $2
				`
		}
		const response = await pool.query(sql,values)
		let data=[]
		if(response.rowCount === 0){
			data = {"message": "Producto no existe o no cumple con los requisitos de la transaccion"}
		}else{
			data = response.rows
		}
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/productosdescripcioncompraventa/:SucursalId/:desc/:SoloInventariable',authenticationToken,async(req,res) => {
	const SucursalId = req.params.SucursalId
	const desc = '%'+req.params.desc+'%'
	const SoloInventariable = 'S'
	let sql=""


	const values = [SucursalId,desc]
	if(SoloInventariable === 'S'){
		sql = `SELECT vw."CodigoId",vw."CodigoBarras",vw."Descripcion", ip."UnidadesInventario",
			ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles"
			FROM vw_productos_descripcion vw 
			INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = vw."CodigoId"
			WHERE ip."SucursalId" = $1
			AND vw."Descripcion" LIKE $2
			AND vw."Inventariable" = 'S'
			`
	}else{
		sql = `SELECT vw."CodigoId",vw."CodigoBarras",vw."Descripcion", ip."UnidadesInventario",
			ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles"
			FROM vw_productos_descripcion vw 
			INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = vw."CodigoId"
			WHERE ip."SucursalId" = $1
			AND vw."Descripcion" LIKE $2
			`
	}

	let data = []
	try{
		const response = await pool.query(sql, values)
		if(response.rowCount === 0){
			data = {"message": "Producto no existe o no cumple con los requisitos de la transaccion"}
		}else{
			data = response.rows
		}
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/comprasconsulta/:SucursalId/:FechaIni/:FechaFin',authenticationToken,async(req,res) => {
	const SucursalId = req.params.SucursalId
	const FechaIni = req.params.FechaIni
	const FechaFin = req.params.FechaFin

	//const values = [SucursalId,FechaIni,FechaFin]
	const values = [SucursalId]
	const sql = `
			SELECT "FolioId","FechaRecepcion",c."ProveedorId",p."Proveedor",c."NumeroFactura",c."TotalFactura",c."SocioId",s."Socio",
			SUM("UnidadesRecibidas") AS "ExtUnidadesRecibidas",
			SUM("CostoCompraSinImpuesto"*"UnidadesRecibidas") AS "ExtCostoCompraSinImp",
			SUM("IVACostoCompra") AS "ExtIVACostoCompra",
			SUM("IEPSCostoCompra") AS "ExtIEPSCostoCompra",
			SUM("CostoCompra"*"UnidadesRecibidas") AS "ExtCostoCompra"
			FROM compras c
			INNER JOIN proveedores p ON c."ProveedorId" = p."ProveedorId"
			INNER JOIN socios s ON c."SocioId" = s."SocioId"
			WHERE "FechaRecepcion" BETWEEN '${FechaIni}' AND '${FechaFin}'
			AND c."Status" = 'R'
			AND c."SucursalId" = $1
			GROUP BY "FolioId","FechaRecepcion",c."ProveedorId",p."Proveedor",c."NumeroFactura",c."TotalFactura",c."SocioId",s."Socio"
			ORDER BY c."FechaRecepcion",c."FolioId";
	`
	try{
		const response = await pool.query(sql, values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message}) 
	}
})

app.get('/api/ventasconsulta/:SucursalId/:FechaIni/:FechaFin',authenticationToken,async (req, res) => {
	const SucursalId = req.params.SucursalId
	const FechaIni = req.params.FechaIni
	const FechaFin = req.params.FechaFin

	const values=[SucursalId, FechaIni, FechaFin]
	const sql = `SELECT v."Fecha",SUM(v."UnidadesVendidas") AS "ExtUnidadesVendidas",SUM(v."PrecioVentaSinImpuesto"*v."UnidadesVendidas") AS "ExtPrecioVentaSinImpuesto",SUM(v."IVAMonto"*v."UnidadesVendidas") AS "ExtIVAMonto",SUM(v."IEPSMonto"*v."UnidadesVendidas") AS "ExtIEPSMonto",SUM(v."PrecioVentaConImpuesto"*v."UnidadesVendidas") AS "ExtPrecioVentaConImpuesto"
			FROM ventas v
			WHERE v."SucursalId" = $1
			AND v."Fecha" BETWEEN $2 AND $3
			GROUP BY v."Fecha"
			ORDER BY v."Fecha"
	`

	try{
		const response = await pool.query(sql, values) 
		const data = await response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})

	}

})

app.get('/api/kardex/:SucursalId/:CodigoBarras/:FechaInicial/:FechaFinal',authenticationToken,async(req, res) => {
	const SucursalId = req.params.SucursalId
	const CodigoBarras = req.params.CodigoBarras
	const FechaInicial = req.params.FechaInicial+" 00:00:00"
	const FechaFinal = req.params.FechaFinal+" 23:59:59"


	let values = [CodigoBarras]
	let sql = ""
	let CodigoId=""
	let data = []
	try{
		sql = `SELECT cb."CodigoId"
			FROM codigos_barras cb
			WHERE cb."CodigoBarras" = $1`
		
		let response = await pool.query(sql,values)
		if(response.rowCount > 0){
			data = await response.rows
			CodigoId= data[0].CodigoId 
		}else{
			res.status(200).json({"error": "Producto No Existe"})
			return
		}


	values=[SucursalId,CodigoId,FechaInicial,FechaFinal]
	sql = `SELECT * FROM vw_kardex
			WHERE "SucursalId" = $1
			AND "CodigoId" = $2
			AND "FechaHora" BETWEEN $3 AND $4 
			ORDER BY "FechaHora" DESC,"SerialId" DESC
	`
		response = await pool.query(sql, values)
		data = await response.rows
	res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/inventarioperpetuo/:SucursalId/:CodigoBarras/:SoloConExistencia',authenticationToken,async (req, res) => {
	const SucursalId = req.params.SucursalId
	let CodigoBarras = req.params.CodigoBarras
	const SoloConExistencia = req.params.SoloConExistencia
	

	if(CodigoBarras === 'novalor'){
		CodigoBarras = ""
	}
	const values = [SucursalId] 
	let sql = `
		SELECT vwpd."CodigoBarras",ip."CodigoId", vwpd."Descripcion",ip."UnidadesInventario", ip."CostoPromedio",
		SUM(ip."UnidadesInventario"*ip."CostoCompra") AS "ExtCostoPromedio"
		FROM inventario_perpetuo ip 
		INNER JOIN vw_productos_descripcion vwpd ON vwpd."CodigoId" = ip."CodigoId" 
		WHERE ip."SucursalId" = $1 `
	if(CodigoBarras){
		sql+=`AND vwpd."CodigoBarras" = '${CodigoBarras}' `
	}else{
		if(SoloConExistencia === 'true'){
			sql+=`AND ip."UnidadesInventario" <> 0 `
		}
	}
		sql+=`GROUP BY vwpd."CodigoBarras",ip."CodigoId",vwpd."Descripcion",ip."UnidadesInventario",ip."CostoPromedio"
		ORDER BY ip."CodigoId"
	`
	try{
		const response = await pool.query(sql,values)
		if(response.rowCount > 0){
			const data = await response.rows	
			res.status(200).json(data)
		}else{
			return res.status(200).json({"error": "Producto No Existe"})
		}

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/api/inventarioperpetuoproductoexistencia/:SucursalId/:CodigoBarras',authenticationToken,async(req, res) => {

	const SucursalId = parseInt(req.params.SucursalId)
	const CodigoBarras = req.params.CodigoBarras

	try{

		const values = [SucursalId,CodigoBarras]
		const sql = `SELECT "UnidadesInventario" 
			FROM inventario_perpetuo ip INNER JOIN codigos_barras cb ON ip."CodigoId" = cb."CodigoId"
			WHERE ip."SucursalId" = $1
			AND cb."CodigoBarras" = $2
		`
		const response = await pool.query(sql,values)

		const data = response.rows

		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ventasfolios/:SucursalId/:Fecha',authenticationToken,async(req, res)=>{
	const SucursalId = req.params.SucursalId
	const Fecha = req.params.Fecha

	const sql = `SELECT "FolioId",COUNT(DISTINCT "CodigoId") AS "Productos",SUM("UnidadesVendidas") AS "Unidades",
			SUM("PrecioVentaConImpuesto"*"UnidadesVendidas") AS "ExtVenta",
			MAX(CAST("FechaHora" AS VARCHAR)) AS "FechaHora" 
		FROM ventas WHERE "SucursalId"=$1 AND "Fecha" = $2
			GROUP BY "FolioId"
			ORDER BY "FolioId"
	`
	const values = [SucursalId, Fecha]
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}

})

app.get('/api/ventasticket/:SucursalId/:FolioId',authenticationToken,async(req, res)=>{
	const SucursalId = req.params.SucursalId
	const FolioId= req.params.FolioId

	const sql = `SELECT v."CodigoId",vw."Descripcion",SUM(v."UnidadesVendidas") AS "UnidadesVendidas",SUM(v."UnidadesVendidas"* v."PrecioVentaConImpuesto") AS "Venta"
		FROM ventas v INNER JOIN vw_productos_descripcion vw ON vw."CodigoId" = v."CodigoId"
		WHERE v."SucursalId"=$1 AND v."FolioId" = $2
		GROUP BY v."CodigoId",vw."Descripcion"
	`
	const values = [SucursalId, FolioId]
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}

})

app.get('/api/ventasconsultafechaproducto/:SucursalId/:FechaInicial/:FechaFinal',authenticationToken,async(req, res) => {
	const SucursalId = req.params.SucursalId	
	const FechaInicial = req.params.FechaInicial
	const FechaFinal = req.params.FechaFinal

	const sql = `SELECT v."CodigoId",v."CodigoBarras",vw."Descripcion",SUM(v."UnidadesVendidas") AS "ExtUnidadesVendidas",
	SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto") AS "ExtVenta"
	FROM ventas v INNER JOIN vw_productos_descripcion vw ON v."CodigoId" = vw."CodigoId"
	WHERE v."SucursalId" = $1 AND v."Fecha" BETWEEN $2 AND $3 
	GROUP BY v."CodigoId",v."CodigoBarras",vw."Descripcion"
	ORDER BY "ExtUnidadesVendidas" DESC
	`
	const values = [SucursalId,FechaInicial,FechaFinal]
	try{
		const response = await pool.query(sql,values)
		const data = await response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/periodoabierto',authenticationToken,async(req,res) => {
	
	const sql = `SELECT "Periodo" FROM cierres_mes WHERE "Status" = 'A'
	`
	try{
		const response = await pool.query(sql)
		if(response.rowCount !== 1){
			res.status(200).json({"error": "No se encontraron Periodos Abierto"});
		}else{
			const data = response.rows
			res.status(200).json(data)
		}
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/colaboradoradministrador/:ColaboradorId',authenticationToken,async(req,res) => {
	const ColaboradorId = req.params.ColaboradorId

	const values = [ColaboradorId] 
	const sql = `SELECT "Administrador" FROM colaboradores WHERE "ColaboradorId" = $1`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/cierremescantidades/:Periodo',authenticationToken,async(req,res) => {
	const Periodo = req.params.Periodo

	try{
		const values=[Periodo]

		const sql = `SELECT s."SucursalId",
				(SELECT COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto"),0) AS "ExtVenta" 
				 FROM ventas
				 WHERE "SucursalId" = s."SucursalId" AND "Fecha" BETWEEN (SELECT DISTINCT "PrimerDiaMes" FROM dim_catalogo_tiempo WHERE "Periodo" = $1)
				 AND (SELECT DISTINCT "UltimoDiaMes" FROM dim_catalogo_tiempo WHERE "Periodo"= $1)),
				(SELECT CAST (COALESCE(SUM("CantidadRetiro"),0) AS DEC(12,2)) AS "CantidadRetiro" 
				 FROM retiros_caja WHERE "SucursalId" = s."SucursalId" AND "Periodo" = $1
				 AND "Status" = 'R'
				 ),
				(SELECT CAST(COALESCE(SUM("CantidadRetiro"),0) AS DEC(12,2)) AS "CantidadRetiroProceso" 
                                 FROM retiros_caja WHERE "SucursalId" = s."SucursalId" AND "Periodo" = $1
				 AND "Status" = 'P'
                                 )
				 FROM sucursales s WHERE "TipoSucursal" = 'S'
				 ORDER BY s."SucursalId"
				`


		let response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
	
})

app.get('/api/fechahoy',authenticationToken,async(req,res) => {
	try{
		const sql = `SELECT CURRENT_DATE AS "FechaHoy"`
		const response = await pool.query(sql)
		const data = response.rows

		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaretiros/:Periodo',authenticationToken,async(req,res) => {
	const Periodo = req.params.Periodo

	try{

	 	const sql = `SELECT rc."SucursalId",rc."FolioId",rc."CantidadRetiro",
				TO_CHAR(rc."FechaHoraGenera", 'yyyy-MM-dd HH24:MI:SS') AS "FechaHoraGenera", 
				TO_CHAR(rc."FechaHoraRecibe", 'yyyy-MM-dd HH24:MI:SS') AS "FechaHoraRecibe", 
				TO_CHAR(rc."FechaHoraCancela", 'yyyy-MM-dd HH24:MI:SS') AS "FechaHoraCancela", 
				c1."User" AS "UserGenera",
				c2."User" AS "UserRecibe",
				c3."User" AS "UserCancela",
				rc."Status"
				FROM retiros_caja rc 
				INNER JOIN colaboradores c1 ON rc."ColaboradorIdGenera" = c1."ColaboradorId"
				LEFT JOIN colaboradores c2 ON rc."ColaboradorIdRecibe" = c2."ColaboradorId"
				LEFT JOIN colaboradores c3 ON rc."ColaboradorIdCancela" = c3."ColaboradorId"
				WHERE "Periodo" = $1
				ORDER BY rc."SucursalId",rc."FolioId"
				`
		const values = [Periodo]
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/api/cargaretiros',authenticationToken,async(req,res) => {
	const SucursalId = req.body.SucursalId
	const Retiro = req.body.Retiro
	const Periodo = req.body.Periodo
	const ColaboradorId = req.body.ColaboradorId
	const Usuario = req.body.Usuario

	const client = await pool.connect()
	let response;
	let values;
	let sql;

	try{
		await client.query('BEGIN')
		values=[SucursalId]
		sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM retiros_caja WHERE "SucursalId" = $1`
		response = await pool.query(sql,values)
		const FolioId = response.rows[0].FolioId

		values=[Periodo]
		sql = `SELECT "PrimerDiaMes" FROM dim_catalogo_tiempo WHERE "Periodo" = $1 `
		response = await pool.query(sql,values)
		const PrimerDiaMes = response.rows[0].PrimerDiaMes

		values = [FolioId,SucursalId,Periodo,PrimerDiaMes,"NOW()",Retiro,"P",ColaboradorId,null,null,null,null,Usuario,"NOW()"]
		sql = `INSERT INTO retiros_caja ("FolioId","SucursalId","Periodo","PrimerDiaMes","FechaHoraGenera","CantidadRetiro","Status","ColaboradorIdGenera",
				"ColaboradorIdRecibe","FechaHoraRecibe","ColaboradorIdCancela","FechaHoraCancela","Usuario","FechaHora")
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
				RETURNING "FolioId","SucursalId"
				`
		response = await client.query(sql, values)
		await client.query('COMMIT')
		res.status(201).json({"message":"Success!!!","SucursalId":response.rows[0].SucursalId,"FolioId":response.rows[0].FolioId})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK') 
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})

app.post('/api/aceptaretiro',authenticationToken,async(req,res) => {
	const SucursalId = req.body.SucursalId
	const FolioId = req.body.FolioId
	const ColaboradorId = req.body.ColaboradorId
	const Usuario = req.body.Usuario

	const client = await pool.connect()
	try{
		await client.query('BEGIN')

		const values = [SucursalId, FolioId, ColaboradorId, Usuario]

		const sql = `UPDATE retiros_caja 
				SET "Status" = 'R', "ColaboradorIdRecibe" = $3,"FechaHoraRecibe" = NOW(),"Usuario"= $4, "FechaHora" = NOW()
				WHERE "SucursalId" = $1
				AND "FolioId" = $2
				AND "Status" = 'P'
				`
		await client.query(sql, values)
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})

app.post('/api/cancelaretiro',authenticationToken,async(req,res) => {
	const SucursalId = req.body.SucursalId
	const FolioId = req.body.FolioId
	const ColaboradorId = req.body.ColaboradorId
	const Usuario = req.body.Usuario

	const client = await pool.connect()
	try{
		await client.query('BEGIN')
		const values = [SucursalId,FolioId,ColaboradorId,Usuario]
		const sql = `UPDATE retiros_caja
				SET "Status" = 'C', "ColaboradorIdCancela" = $3, "FechaHoraCancela" = CLOCK_TIMESTAMP(), "Usuario"= $4, "FechaHora" = CLOCK_TIMESTAMP()
				WHERE "SucursalId" = $1
				AND "FolioId" = $2
				AND "Status" = 'P'
				`
		await client.query(sql, values)
		await client.query('COMMIT')
		res.status(200).json({"messsage": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})

app.post('/api/cierra-abre-mes-retiros',authenticationToken,async(req, res) => {
	const Periodo = req.body.Periodo
	const Usuario = req.body.Usuario


	let anio = Periodo.substr(0,4)
	let mes = Periodo.substr(4,2)
	
	//CALCULA EL NUEVO PERIODO A ABRIR
	let NuevoPerido=0;
	if(parseInt(mes) !== 12){
		mes = (parseInt(mes) + 1).toString().padStart(2,"0")
	}else{
		mes ="1".padStart(2,"0") 
		anio = parseInt(anio)+1
	}
	NuevoPeriodo = anio.toString()+mes.toString()


	const client = await pool.connect()
	try{

		await client.query('BEGIN')

		let values = [NuevoPeriodo]
		let sql = `SELECT DISTINCT "PrimerDiaMes" FROM dim_catalogo_tiempo WHERE "Periodo" = $1`
		let response = await client.query(sql, values)
		const vPrimerDiaMes = response.rows[0].PrimerDiaMes

		values = [Periodo,Usuario]

		sql = `UPDATE cierres_mes 
			SET "Status" = 'C',
			"FechaHora" = CLOCK_TIMESTAMP(),
			"Usuario" = $2
		WHERE "Status" = 'A'AND "Periodo" = $1
		`
		await client.query(sql,values)


		values = [NuevoPeriodo,vPrimerDiaMes,'A',Usuario]
		sql = `INSERT INTO cierres_mes ("Periodo","PrimerDiaMes","Status","FechaHora","Usuario") VALUES ($1,$2,$3,CLOCK_TIMESTAMP(),$4)`
	
		await client.query(sql,values)
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!"})

	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	} finally{
		client.release()
	}
})



app.get('/api/consultaventaspendientes/:SucursalId',authenticationToken,async(req,res) =>{
	const SucursalId = req.params.SucursalId
	try{
		const values = [SucursalId]


		const sql = `SELECT v."SucursalId",v."FolioId",v."SerialId",v."CodigoId",v."UnidadesRegistradas",v."PrecioVentaConImpuesto",
				vw."Descripcion",v."ClienteId",c."Cliente",v."Usuario"
			FROM ventas v 
			INNER JOIN vw_productos_descripcion vw ON v."CodigoId" = vw."CodigoId"
			INNER JOIN vw_clientes c ON c."ClienteId" = v."ClienteId"
		WHERE "SucursalId" = $1 AND v."Status" = 'P'
		ORDER BY v."SucursalId",v."FolioId",v."SerialId"`

		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error) {
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaventaspendientesarreglo/:SucursalId',authenticationToken,async(req,res) =>{
	const SucursalId = req.params.SucursalId
	let FolioId = 0 
	let ClienteId = 0 
	let Cliente = "" 
	let CajeroId = 0 
	let VendedorId = 0
	let Usuario = 0 
	let Status = 'P' 

	let SerialId = 0
	let CodigoId = 0
	let Descripcion = ""
	let UnidadesRegistradas = 0
	let PrecioVentaConImpuesto = 0
	let response2;
	let data2;

	let json;
	let detalles=[]
	let arreglo=[]
	let values=[]
	let sql=""
	try{
		values = [SucursalId]


		sql = `SELECT DISTINCT v."SucursalId",v."FolioId",v."ClienteId",c."Cliente",v."CajeroId",v."VendedorId",v."Usuario",'P' AS "Status"
			FROM ventas v 
			INNER JOIN vw_productos_descripcion vw ON v."CodigoId" = vw."CodigoId"
			INNER JOIN vw_clientes c ON c."ClienteId" = v."ClienteId"
		WHERE "SucursalId" = $1 AND v."Status" = 'P'
		ORDER BY v."SucursalId",v."FolioId"`

		const response = await pool.query(sql,values)
		const data = response.rows
		for (let ii=0; data.length > ii; ii++){
			FolioId = data[ii].FolioId
			ClienteId = data[ii].ClienteId
			Cliente = data[ii].Cliente
			CajeroId = data[ii].CajeroId
			VendedorId = data[ii].Vendedor
			Usuario = data[ii].Usuario
			Status = data[ii].Status

			values = [SucursalId,FolioId]
			sql = `SELECT v."SerialId",v."CodigoId",vw."Descripcion",v."UnidadesRegistradas",v."PrecioVentaConImpuesto"
                        	FROM ventas v
                        	INNER JOIN vw_productos_descripcion vw ON v."CodigoId" = vw."CodigoId"
                        	INNER JOIN vw_clientes c ON c."ClienteId" = v."ClienteId"
                		WHERE "SucursalId" = $1 AND v."FolioId" = $2 
                		ORDER BY v."SerialId"`
			response2 = await pool.query(sql,values)
			data2 = response2.rows
			for (let i=0; data2.length > i; i++) {
				json={
					SerialId: data2[i].SerialId,
					CodigoId: data2[i].CodigoId,
					Descripcion: data2[i].Descripcion,
					UnidadesRegistradas: data2[i].UnidadesRegistradas,
					PrecioVentaConImpuesto: data2[i].PrecioVentaConImpuesto
				}
				detalles.push(json)
			}
			json = {
				SucursalId: SucursalId,
				FolioId: FolioId,
				ClienteId: ClienteId,
				Cliente: Cliente,
				CajeroId: CajeroId,
				VendedorId: VendedorId,
				Usuario: Usuario,
				Status: Status,
				detalles: detalles
			}
			arreglo.push(json)
			detalles = []
		}
		//res.status(200).json(data)
		res.status(200).json(arreglo)
	}catch(error) {
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/api/consultaventapendienteporfolio/:SucursalId/:NotaId',authenticationToken,async(req,res) =>{
	const SucursalId = req.params.SucursalId
	const NotaId = req.params.NotaId
	try{
		const values = [SucursalId,NotaId]
		const sql = `SELECT v.*,vw."Descripcion"
			FROM ventas v  
			INNER JOIN vw_productos_descripcion vw ON v."CodigoId" = vw."CodigoId"
			WHERE v."SucursalId" = $1 AND "FolioId"=$2 AND "Status" = 'P'
			ORDER BY "SerialId"`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error) {
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/api/cancelaventapendiente',authenticationToken,async(req,res) => {
	const SucursalId = req.body.SucursalId
	const FolioId = req.body.FolioId
	const ColaboradorId = req.body.ColaboradorId
	const Usuario = req.body.Usuario

	const client = await pool.connect()
	try{
		await client.query('BEGIN')

		const values = [SucursalId,FolioId,ColaboradorId,Usuario]
		const sql = `UPDATE ventas 
				SET "Status" = 'C',
				"FechaHora" = CLOCK_TIMESTAMP(),
				"CajeroId" = $3,
				"VendedorId" = $3,
				"Usuario" = $4 
				WHERE "SucursalId" = $1
				AND "FolioId" = $2
				AND "Status" = 'P'
		`
		const response = await client.query(sql, values)
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!! Nota Cancelada"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})



app.get('/api/catalogoclientes',authenticationToken,async(req,res) => {

	try{
		const sql = `SELECT * FROM vw_clientes c WHERE "Status" = 'A'`
		const response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data);

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultatipoajustes',authenticationToken,async(req,res) => {
	
	try{
		const sql = `SELECT "TipoAjusteId","Ajuste","Movimiento","AfectaCosto"
				FROM tipo_ajustes WHERE "Aplica" = 'Manual'
				`
		const response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaajustesinventariorecientes/:SucursalId',authenticationToken,async(req,res) => {
	const SucursalId = req.params.SucursalId
	try{
		const values=[SucursalId]
		const sql = `SELECT ai."FolioId",ta."Ajuste",vw."Descripcion",ai."UnidadesAjustadas",ai."FechaHora"
				FROM ajustes_inventario ai 
				INNER JOIN vw_productos_descripcion vw ON ai."CodigoId" = vw."CodigoId"
				INNER JOIN tipo_ajustes ta ON ai."TipoAjusteId" = ta."TipoAjusteId"
				WHERE ai."SucursalId" = $1
				ORDER BY "FolioId" DESC
				LIMIT 10
		`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/api/grabaajustesinventario',authenticationToken,async(req,res) => {
	const { SucursalId,CodigoBarras,TipoAjusteId,AfectaCosto,UnidadesAjustadas,ColaboradorId,Usuario } = req.body

	const client = await pool.connect()
	try{
		let values = [SucursalId,CodigoBarras]
		let sql = `SELECT p."CodigoId",p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",ip."CostoPromedio",
				ip."PrecioVentaSinImpuesto",ip."PrecioVentaConImpuesto"
				FROM productos p
				INNER JOIN codigos_barras cb ON cb."CodigoId" = p."CodigoId"
				INNER JOIN inventario_perpetuo ip ON p."CodigoId" = ip."CodigoId"
				WHERE ip."SucursalId" = $1
				AND cb."CodigoBarras" = $2
			`
		let response = await client.query(sql,values)

		const { CodigoId,CategoriaId,SubcategoriaId,UnidadesInventario,CostoCompra,CostoPromedio,PrecioVentaSinImpuesto,PrecioVentaConImpuesto } = response.rows[0]

		values=[SucursalId]
		sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId"
			FROM ajustes_inventario
			WHERE "SucursalId" = $1
			`
		response = await client.query(sql,values)
		const FolioId = parseInt(response.rows[0].FolioId)

		const UnidadesInventarioAntes = parseInt(UnidadesInventario)
		const UnidadesInventarioDespues = UnidadesInventarioAntes + parseInt(UnidadesAjustadas)

		await client.query('BEGIN')

		values=[SucursalId,CodigoId,FolioId,CodigoBarras,CategoriaId,SubcategoriaId,TipoAjusteId,AfectaCosto,UnidadesAjustadas,UnidadesInventarioAntes,UnidadesInventarioDespues,parseFloat(CostoCompra),parseFloat(CostoPromedio),parseFloat(PrecioVentaSinImpuesto),parseFloat(PrecioVentaConImpuesto),ColaboradorId,Usuario]

		sql = `INSERT INTO ajustes_inventario("SucursalId","CodigoId","FolioId","CodigoBarras","Fecha","CategoriaId","SubcategoriaId","TipoAjusteId",
					"AfectaCosto","UnidadesAjustadas","UnidadesInventarioAntes","UnidadesInventarioDespues","CostoCompra","CostoPromedio",
					"PrecioVentaSinImpuesto","PrecioVentaConImpuesto","ColaboradorId","FechaHora","Usuario")
				VALUES($1,$2,$3,$4,CURRENT_DATE,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CLOCK_TIMESTAMP(),$17)
		`
		await client.query(sql,values)

		values = [SucursalId,CodigoId,UnidadesInventarioDespues,Usuario]
		sql = `UPDATE inventario_perpetuo
 			SET "UnidadesInventario" = $3,
				"FechaHora" = CLOCK_TIMESTAMP(),
				"Usuario" = $4
			WHERE "SucursalId" = $1
			AND "CodigoId" = $2
		`
		await client.query(sql,values)

		await client.query('COMMIT')
		res.status(200).json({"message":"OK","FolioId":FolioId});
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}

})

app.get('/api/consultaventascategorias/:SucursalId/:FechaInicial/:FechaFinal',authenticationToken,async(req,res)=>{
	const SucursalId = req.params.SucursalId
	const FechaInicial = req.params.FechaInicial
	const FechaFinal = req.params.FechaFinal

	try{
		const sql = `SELECT c."CategoriaId",c."Categoria",ROUND(COALESCE(SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto"),0),2) AS "ExtVenta"
				FROM categorias c 
				LEFT JOIN ventas v ON v."CategoriaId" = c."CategoriaId" AND v."Status" = 'V'
						AND v."SucursalId" = $1
						AND v."Fecha" BETWEEN $2 AND $3 
				WHERE 1=1
				GROUP BY c."CategoriaId",c."Categoria"
				ORDER BY c."CategoriaId"
		`
		const values = [SucursalId,FechaInicial,FechaFinal]
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/api/grabatraspasosalida',authenticationToken,async(req,res) => {
	const { SucursalIdOrigen, SucursalIdDestino, ColaboradorIdOrigen,Usuario, detallesPost } = req.body


	const client = await pool.connect()

	try{
		await client.query('BEGIN')
		let sql = ""
		let values = []
		let response;
		let response1;
		let response2;

		let SerialId= 0
		let CodigoId = 0
		let CategoriaId = 0
		let SubcategoriaId = 0
		let StatusOrigen = 'D'
		let FechaOrigen = null
		let CostoCompraOrigen = 0
		let CostoPromedioOrigen = 0
		let PrecioVentaSinImpuestoOrigen = 0
		let PrecioVentaConImpuestoOrigen = 0
		let UnidadesPedidas = 0
    		let UnidadesAsignadas = 0
    		let UnidadesEnviadas = 0
    		let UnidadesInventarioAntesOrigen = 0
    		let UnidadesInventarioDespuesOrigen = 0
    		let FechaHoraOrigen = null
    		let Campo01=null
    		let Campo02=null 
    		let Campo03=null
    		let Campo04=null
    		let FolioIdDestino=0
    		let StatusDestino='T'
    		let FechaDestino=null
    		let CostoCompraDestinoAntes=0
    		let CostoPromedioDestinoAntes=0
    		let CostoCompraDestinoDespues=0
    		let CostoPromedioDestinoDespues=0
    		let PrecioVentaSinImpuestoDestinoAntes=0
    		let PrecioVentaConImpuestoDestinoAntes=0
    		let PrecioVentaSinImpuestoDestinoDespues=0
    		let PrecioVentaConImpuestoDestinoDespues=0
    		let UnidadesRecibidas=0
    		let UnidadesInventarioAntesDestino=0
    		let UnidadesInventarioDespuesDestino=0
    		let FechaHoraDestino=null
    		let ColaboradorIdDestino=null
    		let FechaHora=null
    		let Campo05=null
    		let Campo06=null 
    		let Campo07=null
    		let Campo08=null
		let Margen=0
		let MargenReal=0
		let IVA=null
		let IEPS=null
		let IVAMonto=0
		let IEPSMonto=0

		values = [SucursalIdOrigen]
		sql = `SELECT COALESCE(MAX("FolioIdOrigen"),0) + 1 AS "FolioIdOrigen"
			FROM traspasos
			WHERE "SucursalIdOrigen" = $1
			`
		response = await client.query(sql,values)
		FolioIdOrigen = response.rows[0].FolioIdOrigen

		values = [SucursalIdDestino]
		sql = `SELECT COALESCE(MAX("FolioIdDestino"),0) + 1 AS "FolioIdDestino"
			FROM traspasos
			WHERE "SucursalIdDestino"= $1
			`
		response = await client.query(sql,values)
		FolioIdDestino = response.rows[0].FolioIdDestino

		for (let i = 0; i < detallesPost.length;i++){
			CodigoId = parseInt(detallesPost[i].CodigoId)
			UnidadesPedidas = parseInt(detallesPost[i].UnidadesPedidas)
			UnidadesAsignadas = UnidadesPedidas
			UnidadesEnviadas = UnidadesPedidas

			values = [SucursalIdOrigen,CodigoId]
			sql = `SELECT p."CategoriaId",p."SubcategoriaId",ip."CostoCompra",ip."CostoPromedio",
				ip."PrecioVentaSinImpuesto",ip."PrecioVentaConImpuesto",ip."UnidadesInventario"
				FROM productos p INNER JOIN inventario_perpetuo ip ON p."CodigoId" = ip."CodigoId"
				WHERE ip."SucursalId" = $1
				AND p."CodigoId" = $2
			`
			response = await client.query(sql,values)
			CategoriaId = response.rows[0].CategoriaId
			SubcategoriaId = response.rows[0].SubcategoriaId
			CostoCompraOrigen = parseFloat(response.rows[0].CostoCompra)
			CostoPromedioOrigen = parseFloat(response.rows[0].CostoPromedio)
			PrecioVentaSinImpuestoOrigen = parseFloat(response.rows[0].PrecioVentaSinImpuesto)
			PrecioVentaConImpuestoOrigen = parseFloat(response.rows[0].PrecioVentaConImpuesto)
			UnidadesInventarioAntesOrigen = parseInt(response.rows[0].UnidadesInventario)

			UnidadesInventarioDespuesOrigen = UnidadesInventarioAntesOrigen - UnidadesPedidas


			SerialId= i + 1


			values = [SucursalIdOrigen,FolioIdOrigen,SerialId,CodigoId,CategoriaId,SubcategoriaId,StatusOrigen,CostoCompraOrigen,CostoPromedioOrigen,PrecioVentaSinImpuestoOrigen,PrecioVentaConImpuestoOrigen,UnidadesPedidas,UnidadesAsignadas,UnidadesEnviadas,UnidadesInventarioAntesOrigen,UnidadesInventarioDespuesOrigen,ColaboradorIdOrigen,Campo01,Campo02,Campo03,Campo04,SucursalIdDestino,FolioIdDestino,StatusDestino,FechaDestino,CostoCompraDestinoAntes,CostoPromedioDestinoAntes,CostoCompraDestinoDespues,CostoPromedioDestinoDespues,PrecioVentaSinImpuestoDestinoAntes,PrecioVentaConImpuestoDestinoAntes,PrecioVentaSinImpuestoDestinoDespues,PrecioVentaConImpuestoDestinoDespues,UnidadesRecibidas,UnidadesInventarioAntesDestino,UnidadesInventarioDespuesDestino,FechaHoraDestino,ColaboradorIdDestino,Campo05,Campo06,Campo07,Campo08,Usuario]	



			sql = `INSERT INTO traspasos (
				"SucursalIdOrigen",
 				"FolioIdOrigen",
 				"SerialId",
 				"CodigoId",
 				"CategoriaId",
 				"SubcategoriaId",
 				"StatusOrigen",
 				"FechaOrigen",
 				"CostoCompraOrigen",
 				"CostoPromedioOrigen",
 				"PrecioVentaSinImpuestoOrigen",
 				"PrecioVentaConImpuestoOrigen",
 				"UnidadesPedidas",
 				"UnidadesAsignadas",
 				"UnidadesEnviadas",
 				"UnidadesInventarioAntesOrigen",
 				"UnidadesInventarioDespuesOrigen",
 				"FechaHoraOrigen",
 				"ColaboradorIdOrigen",
 				"Campo01",
 				"Campo02",
 				"Campo03",
 				"Campo04",
 				"SucursalIdDestino",
 				"FolioIdDestino",
 				"StatusDestino",
 				"FechaDestino",
				"CostoCompraDestinoAntes",
 				"CostoPromedioDestinoAntes",
 				"CostoCompraDestinoDespues",
 				"CostoPromedioDestinoDespues",
 				"PrecioVentaSinImpuestoDestinoAntes",
 				"PrecioVentaConImpuestoDestinoAntes",
 				"PrecioVentaSinImpuestoDestinoDespues",
 				"PrecioVentaConImpuestoDestinoDespues",
 				"UnidadesRecibidas",
 				"UnidadesInventarioAntesDestino",
 				"UnidadesInventarioDespuesDestino",
 				"FechaHoraDestino",
 				"ColaboradorIdDestino",
				"Campo05",
				"Campo06",
				"Campo07",
				"Campo08",
				"FechaHora",
				"Usuario") VALUES ($1,$2,$3,$4,$5,$6,$7,CLOCK_TIMESTAMP(),$8,$9,$10,$11,$12,$13,$14,$15,$16,CLOCK_TIMESTAMP(),$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,CLOCK_TIMESTAMP(),$43)`

			await client.query(sql,values)

			//AFECTA tabla inventario_perpetuo en UnidadesInventario de TRASPASO DE SALIDA de Sucursal Origen por Producto
			values = [SucursalIdOrigen,CodigoId,UnidadesEnviadas,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesInventario" = "UnidadesInventario" - $3,
					"FechaHora" = CLOCK_TIMESTAMP(),
					"FechaUltimoTraspasoSalida" = CLOCK_TIMESTAMP(),
					"Usuario" = $4
				WHERE "SucursalId" = $1
				AND "CodigoId" = $2
			`
			await client.query(sql,values)

			//AFECTA tabla inventario_perpetuo UnidadesTransito de TRASPASO DE SALIDA de Sucursal Destino por Producto
			values = [SucursalIdDestino,CodigoId,UnidadesEnviadas,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesTransito" = "UnidadesTransito" + $3,
					"FechaHora" = CLOCK_TIMESTAMP(),
					"Usuario" = $4
				WHERE "SucursalId" = $1
				AND "CodigoId" = $2
			`
			await client.query(sql,values)

		}
		//HASTA AQUÍ ES EL CODIGO DEL PROCESO DE TRASPASO SALIDA


//###############################################################################################################################

		//A PARTIR DE AQUÍ INICIA EL PROCESO DE TRASPASO ENTRADA


		values = [SucursalIdDestino,FolioIdDestino]
		sql = `SELECT "SerialId","CodigoId","UnidadesEnviadas","CostoCompraOrigen","CostoPromedioOrigen"
			FROM traspasos
			WHERE "SucursalIdDestino" = $1
			AND "FolioIdDestino" = $2
		`
		response = await client.query(sql,values)

		for (let i = 0; i < response.rows.length;i++){

			SerialId = parseInt(response.rows[i].SerialId)
			CodigoId = parseInt(response.rows[i].CodigoId)
			UnidadesRecibidas= parseInt(response.rows[i].UnidadesEnviadas)

			//CostoCompraOrigen= parseFloat(response.rows[i].CostoCompraOrigen)

			CostoPromedioOrigen = parseFloat(response.rows[i].CostoPromedioOrigen)
			CostoCompraDestinoDespues = CostoPromedioOrigen


			values = [SucursalIdDestino,CodigoId]
			sql = `SELECT ip."CostoCompra",ip."CostoPromedio",
				ip."PrecioVentaSinImpuesto",ip."PrecioVentaConImpuesto",ip."UnidadesInventario"
				FROM inventario_perpetuo ip 
				WHERE ip."SucursalId" = $1
				AND ip."CodigoId" = $2
			`
			response1 = await client.query(sql,values)
			CostoCompraDestinoAntes = parseFloat(response1.rows[0].CostoCompra)
			CostoPromedioDestinoAntes = parseFloat(response1.rows[0].CostoPromedio)
			PrecioVentaSinImpuestoDestinoAntes = parseFloat(response1.rows[0].PrecioVentaSinImpuesto)
			PrecioVentaConImpuestoDestinoAntes = parseFloat(response1.rows[0].PrecioVentaConImpuesto)
			UnidadesInventarioAntesDestino= parseInt(response1.rows[0].UnidadesInventario)

			UnidadesInventarioDespuesDestino = UnidadesInventarioAntesDestino + UnidadesRecibidas

			CostoPromedioDestinoDespues = ((UnidadesInventarioAntesDestino * CostoPromedioDestinoAntes) + (UnidadesRecibidas * CostoPromedioOrigen)) / (UnidadesInventarioAntesDestino + UnidadesRecibidas)





			if(PrecioVentaConImpuestoDestinoAntes === 0){
				values = [SucursalIdDestino,CodigoId]
				sql = `SELECT "PrecioVentaSinImpuesto", "PrecioVentaConImpuesto", "Margen","IVA","IEPS"
				       FROM inventario_perpetuo 
				       WHERE "SucursalId" <> $1 
				       AND "CodigoId" = $2
				       ORDER BY "PrecioVentaConImpuesto" DESC
				       LIMIT 1
				`
				response2 = await client(sql,values)
				PrecioVentaSinImpuestoDestinoDespues = parseFloat(response2.rows[0].PrecioVentaSinImpuesto)
				PrecioVentaConImpuestoDestinoDespues = parseFloat(response2.rows[0].PrecioVentaConImpuesto)

				Margen = parseFloat(response2.rows[0].Margen)
				IVA = parseFloat(response2.rows[0].IVA)
				IEPS = parseFloat(response2.rows[0].IEPS)

				if (PrecioVentaConImpuestoDestinoDespues === 0){
					PrecioVentaSinImpuestoDestinoDespues = CostoPromedioDestinoDespues / (1-(Margen/100))
					
					PrecioVentaConImpuestoDestinoDespues = Math.ceil(PrecioVentaSinImpuestoDestinoDespues * (1 + ((IVA+IEPS)/100)))
					PrecioVentaSinImpuestoDestinoDespues = PrecioVentaConImpuestoDestinoDespues / (1+((IVA+IEPS)/100))

				}
				IVAMonto = PrecioVentaSinImpuestoDestinoDespues * (IVA/100)
				IEPSMonto = PrecioVentaSinImpuestoDestinoDespus * (IEPS/100)
			}else{
				PrecioVentaSinImpuestoDestinoDespues = PrecioVentaSinImpuestoDestinoAntes
				PrecioVentaConImpuestoDestinoDespues = PrecioVentaConImpuestoDestinoAntes
			}


			MargenReal = ((PrecioVentaSinImpuestoDestinoDespues - CostoPromedioDestinoDespues)/PrecioVentaSinImpuestoDestinoDespues)*100






			values = [SucursalIdDestino,FolioIdDestino,SerialId,CodigoId,CostoCompraDestinoAntes,CostoPromedioDestinoAntes,CostoCompraDestinoDespues,CostoPromedioDestinoDespues,PrecioVentaSinImpuestoDestinoAntes,PrecioVentaConImpuestoDestinoAntes,PrecioVentaSinImpuestoDestinoDespues,PrecioVentaConImpuestoDestinoDespues,UnidadesRecibidas,UnidadesInventarioAntesDestino,UnidadesInventarioDespuesDestino,ColaboradorIdOrigen,Usuario]

			sql = `UPDATE traspasos
				SET "StatusDestino" = 'R',
					"FechaDestino" = CLOCK_TIMESTAMP(),
					"CostoCompraDestinoAntes" = $5,
					"CostoPromedioDestinoAntes" = $6,
					"CostoCompraDestinoDespues" = $7,
					"CostoPromedioDestinoDespues" = $8,
					"PrecioVentaSinImpuestoDestinoAntes" = $9,
					"PrecioVentaConImpuestoDestinoAntes" = $10,
					"PrecioVentaSinImpuestoDestinoDespues" = $11,
					"PrecioVentaConImpuestoDestinoDespues" = $12,
					"UnidadesRecibidas" = $13,
					"UnidadesInventarioAntesDestino" = $14,
					"UnidadesInventarioDespuesDestino" = $15,
					"FechaHoraDestino" = CLOCK_TIMESTAMP(),
					"ColaboradorIdDestino" = $16,
					"FechaHora" = CLOCK_TIMESTAMP(),
					"Usuario" = $17
				WHERE "SucursalIdDestino" = $1
				AND "FolioIdDestino" = $2
				AND "SerialId" = $3
				AND "CodigoId" = $4

			`
			await client.query(sql,values)


			if(PrecioVentaConImpuestoDestinoAntes > 0){

				//OJO AQUI SE TIENEN QUE QUITAR TODAS LAS UNIDADES EN EL PEDIDO UNA VEZ QUE SE RECIBA AUNQUE HAYA FALTANTES
				values = [SucursalIdDestino,CodigoId,UnidadesRecibidas,CostoCompraDestinoDespues,CostoPromedioDestinoDespues,MargenReal,Usuario] 

				sql = `UPDATE inventario_perpetuo
					SET "UnidadesTransito" = "UnidadesTransito" - $3, 
						"UnidadesInventario" = "UnidadesInventario" + $3,
						"CostoCompra"= $4,
						"CostoPromedio" = $5,
						"MargenReal" = $6,
						"FechaUltimoTraspasoEntrada" = CLOCK_TIMESTAMP(),
						"FechaHora" = CLOCK_TIMESTAMP(),
						"Usuario" = $7
					WHERE "SucursalId" = $1
					AND "CodigoId" = $2
			`
			}else{

				values = [SucursalIdDestino,CodigoId,UnidadesRecibidas,CostoCompraDestinoDespues,CostoPromedioDestinoDespues,MargenReal,PrecioVentaSinImpuestoDestinoDespues,PrecioVentaConImpuestoDestinoDespues,IVAMonto,IEPSMonto,Usuario] 
				sql = `UPDATE inventario_perpetuo
					SET "UnidadesTransito" = "UnidadesTransito" - $3, 
						"UnidadesInventario" = "UnidadesInventario" + $3,
						"CostoCompra"= $4,
						"CostoPromedio" = $5,
						"MargenReal" = $6,
						"PrecioVentaSinImpuesto" = $7,
						"PrecioVentaConImpuesto" = $8,
						"IVAMonto" = $9,
						"IEPSMonto" = $10,
						"FechaCambioPrecio" = CLOCK_TIMESTAMP(),
						"FechaUltimoTraspasoEntrada" = CLOCK_TIMESTAMP(),
						"CostoBasePrecioVenta" = CLOCK_TIMESTAMP(),
						"FechaHora" = CLOCK_TIMESTAMP(),
						"Usuario" = $11
					WHERE "SucursalId" = $1
					AND "CodigoId" = $2
			`
			}

			await client.query(sql,values)

		}

		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/codigobarrasprincipal/:CodigoId',authenticationToken,async(req,res) =>{
	const CodigoId = parseInt(req.params.CodigoId)

	const values = [CodigoId]
	const sql = `SELECT "CodigoBarras" FROM productos 
			WHERE "CodigoId" = $1
	`
	try{
		const response = await pool.query(sql,values)

		let data = response.rows 

		if(response.rowsCount === 0){
			data = [{"CodigoBarras": CodigoId}]
		}
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json(data)
	}

})

app.post('/api/inventariociclico',authenticationToken,async(req,res) => {
	const { SucursalId,ColaboradorId, Usuario, detalles } = req.body 

	let values;
	let sql;
	let response;
	let FolioId;
	let CodigoId;
	let CodigoBarras="";
	let UnidadesContadas = 0;
	let UnidadesInventario = 0;
	let UnidadesDiferencia = 0;

	const client = await pool.connect()
	try{
		await client.query('BEGIN')

		values = [SucursalId]
		sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM inventario_ciclico
			WHERE "SucursalId" = $1
			`
		response = await client.query(sql,values)
		FolioId = response.rows[0].FolioId 


		for (let i=0;i<detalles.length;i++){
			CodigoId = detalles[i].CodigoId
			CodigoBarras = detalles[i].CodigoBarras
			UnidadesContadas = detalles[i].UnidadesContadas
			UnidadesInventario = detalles[i].UnidadesInventario
			UnidadesDiferencia = detalles[i].UnidadesDiferencia
			
			values = [SucursalId,FolioId,CodigoId,CodigoBarras,UnidadesContadas,UnidadesInventario,UnidadesDiferencia,ColaboradorId,Usuario]
			sql = `INSERT INTO inventario_ciclico (
				"SucursalId",
 				"FolioId",
 				"CodigoId",
 				"CodigoBarras",
 				"Fecha",
 				"UnidadesContadas",
 				"UnidadesInventario",
 				"UnidadesDiferencia",
 				"ColaboradorId",
 				"Usuario",
 				"FechaHora"
				) VALUES ($1,$2,$3,$4,CURRENT_DATE,$5,$6,$7,$8,$9,CLOCK_TIMESTAMP()) RETURNING "FolioId"
		`

			const response = await client.query(sql,values)
		}
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!","FolioId": response.rows[0].FolioId})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}
})

function authenticationToken(req, res, next) {
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]
    //if (token == null) return res.sendStatus(401)
    if (token == null) return res.status(401).json({"error": "Token No Existe (Acceso No Autorizado)"})
    jwt.verify(token, process.env.ACCESS_TOKEN_SECRET, (err, user2) => {
        //if (err) return res.sendStatus(403)
        if (err) return res.status(403).json({"error": "Token rechazado por Servidor"})
        req.user = user2
        next()
    })
}

const port = parseInt(process.env.PORT)

if (port === 3001){
	app.listen(port, ()=>{console.log(`Server is running.... on Port ${port} PRODUCTION`)})
}else{
	app.listen(port, ()=>{console.log(`Server is running.... on Port ${port} DEVELOPMENT`)})
}
