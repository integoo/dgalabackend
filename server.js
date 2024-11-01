//Para generar .env ACCESS_TOKEN_SECRET=
//node <enter>
//require("crypto").randomBytes(64).toString("hex")

require('dotenv').config()
const express = require('express')
const app = express()
const { Client, Pool } = require('pg')
const bcrypt = require('bcrypt')
const jwt = require('jsonwebtoken') 
const bodyparser = require('body-parser')
const cors = require('cors')
const { restart } = require('nodemon')






/*
//Estas líneas se usan para https mediante el uso de certificados

const fs = require('fs')
const https = require('https')

const options_https = {
	key: fs.readFileSync('/home/ubuntu/produccion/dgalabackend/privkey1.pem'), //Ruta de la Clave o Lleve PRIVADA
	cert: fs.readFileSync('/home/ubuntu/produccion/dgalabackend/fullchain1.pem') //Ruta del Certificado
}

*/








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

/*
//EU ********************************************************
const corsOptions = {
        origin: '*',  // Permitir todos los orígenes
        methods: '*',  // Permitir todos los métodos HTTP
        allowedHeaders: '*',  // Permitir todos los headers
  };

  app.use(cors(corsOptions));

//EU ********************************************************
*/


//routes
app.get('/api/HelloWorld', (req,res)=>{
	res.send('Hello World AWS Testing Server!!!')
})


app.post('/api/login',async (req, res)=>{
	const user = req.body.user
	const password = req.body.password

	if (user === null || user === ''){
		return res.status(401).json({"error":"Usuario No es Válido"})
	}
	if (password === null || password === ''){
		return res.status(401).json({"error":"Password No es Válido"})
	}


	let response="";
	let hashPassword="";
	let values = [user]
	let sql = `SELECT "Password","ColaboradorId","SucursalId",'`+process.env.DB_DATABASE+`' AS db_name,"Administrador","PerfilTransacciones"
			FROM colaboradores 
			WHERE "User" = $1 
			UNION ALL 
			SELECT '0','0','0','0','0','0'
			WHERE NOT EXISTS (
				SELECT 1
				FROM colaboradores
				WHERE "User" = $1)`
	try{
		response = await pool.query(sql, values)
	  	hashPassword = response.rows
	}catch(error){
		console.log(error.message)
		return res.status(500).json({"error": error.message})
	}


	if (hashPassword[0].Password === '0') {
		return res.status(401).json({"error": "Usuario No Existe"})
	}	

	if(await bcrypt.compare(password, hashPassword[0].Password)) {
		//######################################################
		//jwt

		const accessToken = jwt.sign({ name: user }, process.env.ACCESS_TOKEN_SECRET, { expiresIn: '12h' })

		//######################################################


		res.status(200).json({ "error":'',
					"accessToken": accessToken,
					"ColaboradorId": hashPassword[0].ColaboradorId,
					"SucursalId": hashPassword[0].SucursalId,
					"db_name": hashPassword[0].db_name,
					"Administrador":hashPassword[0].Administrador,
					"PerfilTransacciones":hashPassword[0].PerfilTransacciones})
	}else{
		res.status(401).json({"error":"Password Incorrecto"}) 
	}
})


app.get('/api/sucursales/:naturalezaCC',authenticationToken,async(req, res) => {
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

app.get('/api/fechaactual',authenticationToken,async(req,res) =>{
	try{
		const sql = `SELECT "Fecha","PrimerDiaMes","UltimoDiaMes"
			FROM dim_catalogo_tiempo
			WHERE "Fecha" = CURRENT_DATE 
			`
		const response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ingresos/unidadesdenegociocatalogo/:naturalezaCC',authenticationToken,async(req, res) => {
	let naturalezaCC = req.params.naturalezaCC;

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

app.get('/api/ingresos/unidadesdenegocio/:sucursal',authenticationToken,async(req, res) => {
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

app.get('/api/ingresos/cuentascontablescatalogo/:naturalezaCC',authenticationToken, async(req, res) => {
	let naturalezaCC = req.params.naturalezaCC;


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

app.get('/api/ingresos/cuentascontables/:sucursal/:unidaddenegocio',authenticationToken, async(req, res) => {
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

app.get('/api/ingresos/subcuentascontablescatalogo/:naturalezaCC',authenticationToken, async(req,res) => {
	const naturalezaCC = req.params.naturalezaCC;

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


app.get('/api/ingresos/subcuentascontables/:sucursal/:unidaddenegocio/:cuentacontable',authenticationToken, async(req, res) => {
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

app.post('/api/ingresos/grabaingresos',authenticationToken, async(req, res) => {

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
	let response='';
	let Periodo;

	try{
		await client.query('BEGIN')
		values = [vfecha]
		sql = `SELECT "Periodo" FROM dim_catalogo_tiempo WHERE "Fecha" = $1`
		response = await client.query(sql,values)
		Periodo = response.rows[0].Periodo

		
		values = [vsucursalid,vunidaddenegocioid,vcuentacontableid,vsubcuentacontableid,vcomentarios,vfecha,Periodo,vmonto,'P',"now()",vusuario,'now()']
		sql = `INSERT INTO registro_contable ("FolioId","SucursalId","UnidadDeNegocioId","CuentaContableId",
				"SubcuentaContableId","Comentarios","Fecha","Periodo","Monto","Moneda",
				"FechaHoraAlta","Usuario","FechaHora") VALUES (
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
		response = await client.query(sql,values)
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!! FOLIO="+response.rows[0].FolioId})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		// res.status(400).send(error.message)
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
	
})

app.post('/api/ingresos/grabaingresos2',authenticationToken, async(req, res) => {
	const arreglo = req.body
	let values = []
	const client = await pool.connect();
	let sql = ''
	let response;
	let Periodo;
	try{
		await client.query('BEGIN')
		values = [arreglo[0].Fecha]
		sql = `SELECT "Periodo" FROM dim_catalogo_tiempo WHERE "Fecha" = $1`
		response = await client.query(sql,values)
		Periodo = response.rows[0].Periodo
		for (let i =0; i< arreglo.length; i++){
			const vsucursalid = parseInt(arreglo[i].SucursalId)
			const vunidaddenegocioid = parseInt(arreglo[i].UnidadDeNegocioId)
			const vcuentacontableid = parseInt(arreglo[i].CuentaContableId)
			const vsubcuentacontableid = arreglo[i].SubcuentaContableId
			const vfecha = arreglo[i].Fecha
			const vmonto = parseFloat(arreglo[i].Monto)
			const vcomentarios = arreglo[i].Comentarios
			const vusuario = arreglo[i].Usuario

			values = [vsucursalid,vunidaddenegocioid,vcuentacontableid,vsubcuentacontableid,vcomentarios,vfecha,Periodo,vmonto,'P',"now()",vusuario,'now()']
			sql = `INSERT INTO registro_contable ("FolioId","SucursalId","UnidadDeNegocioId","CuentaContableId",
			"SubcuentaContableId","Comentarios","Fecha","Periodo","Monto","Moneda",
			"FechaHoraAlta","Usuario","FechaHora") VALUES (
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
			//console.log(response.rows[0].SucursalId,response.rows[0].FolioId)
		}
		await client.query('COMMIT')
		res.status(200).json({"message": "Success!!!"})
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(400).send(error.message)
	}finally{
		client.release()
	}
})



app.get('/api/ingresos/getIngresosEgresos/:fecha/:naturalezaCC/:accesoDB/:trans',authenticationToken,async(req, res)=> {
	const vfecha = req.params.fecha
	const naturalezaCC = req.params.naturalezaCC
	const accesoDB = req.params.accesoDB
	const trans = req.params.trans
	
	const values=[vfecha]

	let sql = `SELECT rc."Id",rc."SucursalId",rc."FolioId",s."SucursalNombre",udn."UnidadDeNegocioId",udn."UnidadDeNegocioNombre",cc."CuentaContableId",
		cc."CuentaContable",scc."SubcuentaContableId",
		scc."SubcuentaContable",CAST(rc."Fecha" AS CHAR(10)),rc."Monto", rc."Comentarios"
		FROM registro_contable rc
		INNER JOIN sucursales s ON rc."SucursalId"=s."SucursalId"
		INNER JOIN unidades_de_negocio udn ON rc."UnidadDeNegocioId"=udn."UnidadDeNegocioId"
		INNER JOIN cuentas_contables cc ON rc."CuentaContableId" = cc."CuentaContableId"
		INNER JOIN subcuentas_contables scc ON rc."CuentaContableId"= scc."CuentaContableId" AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		INNER JOIN dim_catalogo_tiempo dct ON dct."Fecha" = rc."Fecha"
		`
	if (accesoDB === 'mes'){
		sql+=`WHERE dct."PrimerDiaMes"<= $1 AND dct."UltimoDiaMes">= $1 `
	}else{
		sql+=`WHERE rc."Fecha" = $1 `
	}

	if(naturalezaCC > 0){
		sql+=`AND rc."Monto" >= 0 AND cc."NaturalezaCC" = 1 `
	}else{
		sql+=`AND rc."Monto" <= 0 AND cc."NaturalezaCC" = -1 `
	}
	if (trans === "Ingresos"){
		sql+=`ORDER BY rc."Fecha" DESC,rc."SucursalId",rc."FolioId" DESC,udn."UnidadDeNegocioId",cc."CuentaContableId",scc."SubcuentaContableId"`
	}else{
		sql+=`ORDER BY rc."Id" DESC,rc."SucursalId",rc."FolioId" DESC,udn."UnidadDeNegocioId",cc."CuentaContableId",scc."SubcuentaContableId"`
	}
	
	let response;
	try{
		response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.put('/api/actualizaingresosegresos',authenticationToken,async(req, res) =>{
	const SucursalId = parseInt(req.body.SucursalId)
	const FolioId = parseInt(req.body.FolioId)
	const Monto = parseFloat(req.body.Monto) 
	const Comentarios = req.body.Comentarios 
	const Usuario = req.body.Usuario

	let response;
	let values=[];
	let sql="";

	const client = await pool.connect()
	try{
		await client.query('BEGIN')

		values = [SucursalId,FolioId,Monto,Comentarios,Usuario]
		sql = `UPDATE registro_contable
				SET "Monto" = $3,
				"Comentarios" = $4,
				"Usuario" = $5,
				"FechaHora" = CLOCK_TIMESTAMP()
				WHERE "SucursalId" = $1
				AND "FolioId" = $2
				AND "Fecha" IN (
					SELECT dct."Fecha" FROM dim_catalogo_tiempo dct INNER JOIN cierres_mes cm ON dct."Fecha" >= cm."PrimerDiaMes" AND cm."Status" = 'A'
				)
				`
		response = await client.query(sql,values)
		if(response.rowCount === 0){
			res.status(201).json({"message": "NO SE ACTUALIZÓ EL REGISTRO!!!"})
		}else{
			res.status(201).json({"message": "Success!!!"})
		}
		await client.query('COMMIT')
	}catch(error){
		console.log(error.message)
		client.query('ROLLBACK')
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})

app.get('/api/periodoabierto',authenticationToken,async (req, res) => {
	let sql = `SELECT cm."Periodo",cm."PrimerDiaMes",CAST(ct."UltimoDiaMes" AS CHAR(10)) 
			FROM cierres_mes cm 
			INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = cm."PrimerDiaMes" 
			WHERE cm."Status" = 'A' 
	`
	let response;
	const values=[]
	try{
		const data = await pool.query(sql, values)
		res.status(200).json(data.rows)
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
		sql = `SELECT "SucursalId","Sucursal" FROM sucursales WHERE "Status" = 'A' AND "TipoSucursal" IN ('S','C') ORDER BY "SucursalId"`
	}
	if (id === '10fisicasycedis'){
		sql = `
                       SELECT "SucursalId","Sucursal" 
                       FROM sucursales WHERE "Status" = 'A' AND "TipoSucursal" IN ('S','C')
                       AND "SucursalId" != 4 ORDER BY "SucursalId"`
	}
	if (id === '10todasyfisicas'){
		sql = `
                       SELECT '00' AS "SucursalId",'00 TODAS' AS "Sucursal"
                       UNION ALL
                       SELECT "SucursalId","Sucursal" 
                       FROM sucursales WHERE "Status" = 'A' AND "TipoSucursal" IN ('S')
                       AND "SucursalId" != 4 ORDER BY "SucursalId"`
	}
	if (id === '10fisicas'){
		sql = `
                       SELECT "SucursalId","Sucursal" 
                       FROM sucursales WHERE "Status" = 'A' AND "TipoSucursal" IN ('S')
                       AND "SucursalId" != 4 ORDER BY "SucursalId"`
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

app.get('/api/validamovimientoingresosegresos/:SucursalId/:UnidadDeNegocioId/:CuentaContableId/:SubcuentaContableId/:Fecha',authenticationToken,async(req,res) => {
	const SucursalId = parseInt(req.params.SucursalId)
	const UnidadDeNegocioId = parseInt(req.params.UnidadDeNegocioId)
	const CuentaContableId = parseInt(req.params.CuentaContableId)
	const SubcuentaContableId = req.params.SubcuentaContableId
	const Fecha = req.params.Fecha

	const values = [SucursalId,UnidadDeNegocioId,CuentaContableId,SubcuentaContableId,Fecha]
	const sql = `SELECT COUNT(*) AS "cuantos"
			FROM registro_contable
			WHERE "SucursalId" = $1
			AND "UnidadDeNegocioId" = $2
			AND "CuentaContableId" = $3
			AND "SubcuentaContableId" = $4
			AND "Fecha" = $5
	`
	try{
		const response = await pool.query(sql,values)
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
		sql = `SELECT "SucursalId" FROM sucursales WHERE "TipoSucursal" IN ('S','C')`
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


		values = [SucursalId] 
		let sql = `SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM compras WHERE "SucursalId" = $1`
		let response = await client.query(sql,values) 
		let FolioId = response.rows[0].FolioId

		values = [Usuario]
		sql = `SELECT "ColaboradorId" FROM colaboradores WHERE "User" = $1 ` 
		response = await client.query(sql,values)
		let ColaboradorId = response.rows[0].ColaboradorId


		for (let i=0; i < detalles.length; i++){

			CodigoId = detalles[i].CodigoId

			values = [SucursalId,CodigoId]
			sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",p."IVAId",ii."IVA",p."IEPSId",iie."IEPS",
			ip."CostoPromedio",ip."Margen",ip."PrecioVentaSinImpuesto",
			ip."PrecioVentaConImpuesto"
			FROM productos p
			INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = p."CodigoId"
			INNER JOIN impuestos_iva ii ON ii."IVAId" = p."IVAId"
			INNER JOIN impuestos_ieps iie ON iie."IEPSId" = p."IEPSId"
			WHERE ip."SucursalId" = $1  
			AND  p."CodigoId" = $2 
		`
			response = await client.query(sql,values)
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

			if(UnidadesInventario >=0){
				CostoPromedio = ((parseInt(UnidadesRecibidas) * parseFloat(CostoCompra)) + (parseInt(UnidadesInventario)*parseFloat(CostoCompraAnt))) / (parseInt(UnidadesRecibidas) + parseInt(UnidadesInventario))
			}else{
				CostoPromedio = ((parseInt(UnidadesRecibidas) * parseFloat(CostoCompra)) + (parseInt(0)*parseFloat(CostoCompraAnt))) / (parseInt(UnidadesRecibidas) + parseInt(0))
			}

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

				  let banderaActualizaPrecioSucursales = false


				  if(parseFloat(response.rows[0].PrecioVentaConImpuesto) > 0){
				  	banderaActualizaPrecioSucursales = false
					NuevoPrecioVentaConImpuesto = parseFloat(response.rows[0].PrecioVentaConImpuesto)
					NuevoPrecioVentaSinImpuesto = NuevoPrecioVentaConImpuesto / (1+((IVAVenta+IEPS)/100)) 
					IVAMonto = (NuevoPrecioVentaSinImpuesto * (IVAVenta/100))
					IEPSMonto = (NuevoPrecioVentaSinImpuesto * (IEPS/100))
				  }else{
				  	banderaActualizaPrecioSucursales = true 
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
				await client.query(sql,values)


				//Actualiza el Precio de Venta de Otras sucursales que tengan Precio de Venta CERO
				if (banderaActualizaPrecioSucursales){
					values = [SucursalId, CodigoId, NuevoPrecioVentaConImpuesto,Usuario]
					sql = `UPDATE inventario_perpetuo
						SET "PrecioVentaConImpuesto" = $3,
						"FechaCambioPrecio" = NOW(),
						"FechaHora" = NOW(),
						"Usuario" = $4
						WHERE "SucursalId" != $1 AND "CodigoId" = $2 AND "PrecioVentaConImpuesto" = 0
					`
					await client.query(sql,values)

				}

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
				await client.query(sql,values)
			}
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

	const sql = `INSERT INTO public.ventas ("SucursalId", "FolioId", "CodigoId", "SerialId", "Fecha", "FolioCompuesto", "Status", "ClienteId","CajeroId","VendedorId","CodigoBarras", "CategoriaId", "SubcategoriaId","FolioIdInventario","UnidadesRegistradas", "UnidadesVendidas", "UnidadesInventarioAntes", "UnidadesInventarioDespues", "CostoCompra", "CostoPromedio", "PrecioVentaSinImpuesto", "IVAId", "IVA", "IVAMonto", "IEPS", "IEPSMonto", "PrecioVentaConImpuesto", "UnidadesDevueltas","FechaDevolucionVenta","ComisionVentaPorcentaje","ComisionVenta","FechaHoraAlta","FechaHora","Usuario","CostoPromedioOriginal","CostoPromedioProcesado") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,$23, $24, $25, $26, $27, $28, $29, $30, $31, CLOCK_TIMESTAMP(), CLOCK_TIMESTAMP(),$32,$33,$34) RETURNING "FolioId"`

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
		let ComisionVentaPorcentaje=0;
		let ComisionVenta=0;
		let respuesta;
		let UnidadesRegistradas = 0;


		for (let i=0; i < detalles.length; i++){

			CodigoId = parseInt(detalles[i].CodigoId)

			values = [SucursalId, CodigoId]
			sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",p."IVAId",ii."IVA",p."IEPSId",iie."IEPS",
				ip."CostoPromedio",ip."Margen",ip."PrecioVentaSinImpuesto",p."Inventariable",p."ComisionVentaPorcentaje"
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
			if(Inventariable !== 'S'){
				UnidadesInventarioAntes = 0 
				UnidadesInventarioDespues = 0
			}


			ComisionVentaPorcentaje = parseFloat(response.rows[0].ComisionVentaPorcentaje)


			IVAId = response.rows[0].IVAId
			IVA = response.rows[0].IVA
			IEPS = response.rows[0].IEPS


 			// CodigoId 65 es el "SERVICIO POR ENCARGO" de lavamatica y se calcula su Precio en base a un % sobre la venta de Productos de CategoriaId = 3
			if(CodigoId === 65){
				PrecioVentaSinImpuesto = parseFloat(detalles[i].PrecioVentaConImpuesto) /(1+((parseFloat(IVA) + parseFloat(IEPS))/100))
			}else{
				PrecioVentaSinImpuesto = response.rows[0].PrecioVentaSinImpuesto
				ComisionVenta = 0
			}

			IVAMonto = parseFloat(PrecioVentaSinImpuesto) * parseFloat(IVA/100) 
			IEPSMonto = parseFloat(PrecioVentaSinImpuesto) * parseFloat(IEPS/100) 
			PrecioVentaConImpuesto = detalles[i].PrecioVentaConImpuesto
			//FechaHora= 'NOW()'
			UnidadesDevueltas = 0
			FechaDevolucionVenta = null

			if(parseFloat(ComisionVentaPorcentaje) !== 0){
				ComisionVenta = PrecioVentaSinImpuesto * (ComisionVentaPorcentaje/100) 
			}



			values = [SucursalId, FolioId, CodigoId, SerialId, Fecha, FolioCompuesto, Status, ClienteId, CajeroId, VendedorId, CodigoBarras, CategoriaId, SubcategoriaId, FolioIdInventario, UnidadesRegistradas, UnidadesVendidas,UnidadesInventarioAntes, UnidadesInventarioDespues, CostoCompra,CostoPromedio,PrecioVentaSinImpuesto,IVAId,IVA,IVAMonto,IEPS,IEPSMonto,PrecioVentaConImpuesto,UnidadesDevueltas, FechaDevolucionVenta, ComisionVentaPorcentaje, ComisionVenta,Usuario,CostoPromedio,'N']


			sql = sqlventasinsert()  //Manda llamar el query sql para insertar a la tabla de ventas

			respuesta = await client.query(sql, values)

		if (Status === 'V' && Inventariable === 'S'){
			values = [SucursalId, CodigoId, UnidadesVendidas,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesInventario" = "UnidadesInventario" - $3,
					"FechaUltimaVenta" = NOW(),
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
			Usuario,
			CostoPromedio,
			'N']
			


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
					"FechaUltimaVenta" = CURRENT_DATE,
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
	const sql = `SELECT vw."CodigoId",vw."Descripcion",ip."PrecioVentaConImpuesto",ip."CostoPromedio",vw."Inventariable",vw."CompraVenta",vw."CategoriaId",
			ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles"
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

app.get('/api/productosdescripcion/:desc/:SucursalId',authenticationToken,async(req,res) => {
	const desc = '%'+req.params.desc+'%'
	const SucursalId = req.params.SucursalId
/*
	const sql = `SELECT vw."CodigoId",vw."CodigoBarras",vw."Descripcion" FROM vw_productos_descripcion vw
			WHERE vw."Descripcion" LIKE $1 
			AND vw."CompraVenta" IN ('A','V')
	`
*/

	const sql = `SELECT vw."CodigoId",vw."CodigoBarras",vw."Descripcion", ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles",
				ip."PrecioVentaConImpuesto"
			FROM vw_productos_descripcion vw INNER JOIN inventario_perpetuo ip ON vw."CodigoId" = ip."CodigoId"
			WHERE vw."Descripcion" LIKE $1 
			AND vw."CompraVenta" IN ('A','V')
			AND ip."SucursalId" = $2
	`


	const values = [desc,SucursalId]
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

app.get('/api/inventarioperpetuo/:SucursalId/:CodigoBarras/:SoloConExistencia/:radiovalue',authenticationToken,async (req, res) => {
	const SucursalId = req.params.SucursalId
	let CodigoBarras = req.params.CodigoBarras
	const SoloConExistencia = req.params.SoloConExistencia
	const radiovalue = req.params.radiovalue
	

	if(CodigoBarras === 'novalor'){
		CodigoBarras = ""
	}
	const values = [SucursalId] 
	let sql = `
		SELECT vwpd."CodigoBarras",ip."CodigoId", vwpd."Descripcion",ip."UnidadesInventario", ip."CostoPromedio",vwpd."CategoriaId",
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
		sql+=`GROUP BY vwpd."CodigoBarras",ip."CodigoId",vwpd."Descripcion",ip."UnidadesInventario",ip."CostoPromedio",vwpd."CategoriaId"
	`
	if (radiovalue ==="porcodigo"){
		sql+=`ORDER BY ip."CodigoId"`
	}
	if (radiovalue ==="porunidadesinventario"){
		sql+=`ORDER BY ip."UnidadesInventario" DESC`
	}
	if (radiovalue ==="porcategoriaunidadesinventario"){
		sql+=`ORDER BY vwpd."CategoriaId",ip."UnidadesInventario" DESC`
	}


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
			 	"FechaUltimoAjuste" = CURRENT_DATE,
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

			if(UnidadesInventarioAntesDestino >= 0){
				CostoPromedioDestinoDespues = ((UnidadesInventarioAntesDestino * CostoPromedioDestinoAntes) + (UnidadesRecibidas * CostoPromedioOrigen)) / (UnidadesInventarioAntesDestino + UnidadesRecibidas)
			}else{
				CostoPromedioDestinoDespues = ((0 * CostoPromedioDestinoAntes) + (UnidadesRecibidas * CostoPromedioOrigen)) / (0 + UnidadesRecibidas)

			}


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
						"Margen" = (CASE WHEN "Margen" = 100 THEN $6 ELSE "Margen" END),
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
						"Margen" = (CASE WHEN "Margen" = 100 THEN $6 ELSE "Margen" END),
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
	}finally{
		client.release()
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
	}finally{
		client.release()
	}
})


app.get('/api/ventassucursaleshoy',authenticationToken,async(req,res) => {
	try{
		const sql = `SELECT s."Sucursal",SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto") AS "ExtVentaConImp"
			FROM vw_ventas_devoventas v INNER JOIN sucursales s ON v."SucursalId" = s."SucursalId"
			WHERE v."Fecha" = CURRENT_DATE
			GROUP BY s."Sucursal"
			ORDER BY s."Sucursal"
		`
		const response = await pool.query(sql)
		const data = response.rows

		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ventassucursalesperiodolavamatica/:FechaInicial/:FechaFinal/:DiasMes',authenticationToken,async(req,res)=>{
	const FechaInicial = req.params.FechaInicial 
	const FechaFinal = req.params.FechaFinal
	const DiasMes = parseInt(req.params.DiasMes)

	//Días que se toman de base del periodo
	const d = new Date(FechaFinal)
	const dias = parseInt(d.getDate())

	const values = [FechaInicial,FechaFinal,dias,DiasMes]
	
	const sql = `SELECT s."Sucursal",
		COALESCE(SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto"),0) AS "Venta",
		CAST(COALESCE(SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto"),0)/$3*$4 AS DEC(12,2)) AS "VentaProyectada",
		COALESCE(cm."Cuota",0) AS "Cuota",
	
		CAST(((COALESCE(SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto"),0)/$3*$4) -
		COALESCE(cm."Cuota",0)) AS DEC(12,2))  AS "DiferenciaDinero",
	
		
		CASE WHEN COALESCE(cm."Cuota",0) = 0
		THEN 
			100
		ELSE
			CAST( (COALESCE(SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto"),0)/$3*$4 / 
			CASE WHEN COALESCE(cm."Cuota",0) = 0 THEN 1 ELSE COALESCE(cm."Cuota",0) END)*100 AS DEC(5,2))
		END
		AS "DiferenciaPorcentaje"

		FROM sucursales s
		LEFT JOIN ventas v ON v."SucursalId" = s."SucursalId" AND v."Status" = 'V' AND v."Fecha" BETWEEN $1 AND $2
		LEFT JOIN cuotas_mes cm ON cm."SucursalId" = s."SucursalId" AND cm."PrimerDiaMes" BETWEEN $1 AND $2 AND cm."UnidadDeNegocioId" IN (3,5)
		WHERE s."TipoSucursal" = 'S' AND s."Status" = 'A'
		GROUP BY s."Sucursal",COALESCE(cm."Cuota",0)
		ORDER BY s."Sucursal"
		`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ventassucursalesperiodounidaddenegocio/:FechaInicial/:FechaFinal/:DiasMes/:UnidadDeNegocioId',authenticationToken,async(req,res) =>{
	const FechaInicial = req.params.FechaInicial 
	const FechaFinal = req.params.FechaFinal 
	const DiasMes = req.params.DiasMes
	const UnidadDeNegocioId = req.params.UnidadDeNegocioId

	//Días que se toman de base del periodo
	const d = new Date(FechaFinal)
	const dias = parseInt(d.getDate())

	try{
		const values = [FechaInicial,FechaFinal,dias,DiasMes,UnidadDeNegocioId]

		const sql = `SELECT s."Sucursal",
			COALESCE(SUM("Monto"),0) AS "Venta",
			CAST(COALESCE(SUM("Monto"),0)/$3*$4 AS DEC(12,2)) AS "VentaProyectada",
			COALESCE(cm."Cuota",0) AS "Cuota",

			CAST(((COALESCE(SUM("Monto"),0)/$3*$4) -
			COALESCE(cm."Cuota",0)) AS DEC(12,2))  AS "DiferenciaDinero",

			CASE WHEN COALESCE(cm."Cuota",0) = 0
			THEN 
				100
			ELSE
				CAST( (COALESCE(SUM("Monto"),0)/$3*$4 / 
				CASE WHEN COALESCE(cm."Cuota",0) = 0 THEN 1 ELSE COALESCE(cm."Cuota",0) END)*100 AS DEC(5,2))
			END
			AS "DiferenciaPorcentaje",
			MAX(rc."Fecha") AS "FechaMaxima"


			FROM registro_contable rc 
			INNER JOIN sucursales s ON s."SucursalId" = rc."SucursalId"
			LEFT JOIN cuotas_mes cm ON cm."SucursalId" = s."SucursalId" AND cm."PrimerDiaMes" BETWEEN $1 AND $2 AND cm."UnidadDeNegocioId" IN ($5)
			WHERE rc."Fecha" BETWEEN $1 AND $2 AND rc."UnidadDeNegocioId" = $5 AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = 1) 
			GROUP BY s."Sucursal", cm."Cuota"
			ORDER BY s."Sucursal"
		`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaperiodos',authenticationToken,async(req,res) =>{
	let sql = `SELECT DISTINCT "Periodo","PrimerDiaMes","UltimoDiaMes",CURRENT_DATE AS "Hoy",
				CURRENT_DATE -1 AS "Ayer" 
				FROM dim_catalogo_tiempo
				WHERE ("Fecha" IN (SELECT DISTINCT "Fecha" FROM ventas WHERE "Status" = 'V') OR
				"Fecha" = CURRENT_DATE)
				ORDER BY "Periodo" DESC
				`
	try{
		const response = await pool.query(sql)
		const data = response.rows 
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaperiodosregistrocontable',authenticationToken,async(req,res) =>{
	let sql = `SELECT DISTINCT "Periodo","PrimerDiaMes","UltimoDiaMes",CURRENT_DATE AS "Hoy",
				CURRENT_DATE -1 AS "Ayer" 
				FROM dim_catalogo_tiempo
				WHERE ("Fecha" IN (SELECT DISTINCT "Fecha" FROM registro_contable) OR
				"Fecha" = CURRENT_DATE)
				ORDER BY "Periodo" DESC
				`
	try{
		const response = await pool.query(sql)
		const data = response.rows 
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/api/consultaproductospadres/:SucursalId/:DescripcionPadre',authenticationToken,async(req,res) =>{
	const SucursalId = parseInt(req.params.SucursalId)
	let DescripcionPadre = req.params.DescripcionPadre

	try{
		let values = [SucursalId]
		//Aqui falta poner la validacion del Status en inventario_perpetuo por sucursal
		let sql = `SELECT DISTINCT vw."CodigoId",vw."CodigoBarras",vw."Descripcion",ip."UnidadesInventario",
				ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles",
				(SELECT COUNT(*) FROM cambios_presentacion cp WHERE cp."CodigoIdPadre" = vw."CodigoId") AS "CantidadHijos"
				FROM vw_productos_descripcion vw INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = vw."CodigoId"
				WHERE vw."CodigoId" IN (SELECT "CodigoIdPadre" FROM cambios_presentacion)
				AND ip."SucursalId" = $1 AND vw."CodigoId" IN (SELECT "CodigoId" FROM productos WHERE "Status" != 'C') 
		`

		if(DescripcionPadre !== 'null' && DescripcionPadre !== ""){
			DescripcionPadre = '%'+DescripcionPadre+'%'
			sql +=`AND vw."Descripcion" LIKE $2`
			values = [SucursalId,DescripcionPadre]
		}
		const response = await pool.query(sql,values)
		const data = response.rows


		let response2;
		let data2;
		let arregloPadre = []
		let arregloHijos = []
		let json;


		//En este ciclo busca los hijos de cada padre
		for (let i=0; data.length > i;i++){
			values=[SucursalId,parseInt(data[i].CodigoId)]
			//Aqui falta poner la validacion del Status en inventario_perpetuo por sucursal
			sql = `SELECT cp."CodigoIdHijo",vw."CodigoBarras" AS "CodigoBarrasHijo",vw."Descripcion" AS "DescripcionHijo",
				cp."FactorConversion",ip."UnidadesInventario" AS "UnidadesInventarioHijo"
				FROM cambios_presentacion cp 
				INNER JOIN vw_productos_descripcion vw ON vw."CodigoId" = cp."CodigoIdHijo"
				INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = cp."CodigoIdHijo" AND ip."SucursalId" = $1
				WHERE cp."CodigoIdPadre" = $2
				AND cp."CodigoIdHijo" IN (SELECT "CodigoId" FROM productos WHERE "Status" != 'C') 
			`
			response2 = await pool.query(sql,values)
			data2 = response2.rows

			arregloHijos = []
			for(let ii=0; data2.length > ii; ii++){
				json ={
					"CodigoIdHijo": data2[ii].CodigoIdHijo,
					"CodigoBarrasHijo": data2[ii].CodigoBarrasHijo,
					"DescripcionHijo": data2[ii].DescripcionHijo,
					"FactorConversion": data2[ii].FactorConversion,
					"UnidadesInventarioHijo": data2[ii].UnidadesInventarioHijo,
				}
				arregloHijos.push(json)
			}

			json = {
				"CodigoIdPadre":data[i].CodigoId,
				"CodigoBarrasPadre": data[i].CodigoBarras,
				"DescripcionPadre": data[i].Descripcion,
				"UnidadesInventarioPadre": data[i].UnidadesInventario,
				"UnidadesDisponiblesPadre": data[i].UnidadesDisponibles,
				"CantidadHijos": data[i].CantidadHijos,
				"detalles": arregloHijos 
			}
			arregloPadre.push(json)
		}

		res.status(200).json(arregloPadre)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.post('/api/cambiosdepresentacionajustes',authenticationToken,async(req,res) => {
	const { SucursalId,CodigoIdPadre,CodigoBarrasPadre,UnidadesConvertir,FactorConversion,CodigoIdHijo,CodigoBarrasHijo,UnidadesHijoRecibe,ColaboradorId,Usuario } = req.body

	let sql;
	let values=[]
	let response;
	let data;
	let FolioId=0
	let CategoriaId = 0
	let SubcategoriaId = 0
	let UnidadesInventario = 0
	let UnidadesInventarioDespues = 0
	let CostoCompra = 0
	let CostoPromedio = 0
	let CostoPromedioPadre = 0
	let PrecioVentaSinImpuesto = 0
	let PrecioVentaConImpuesto = 0
	let Margen = 0
	let MargenReal = 0
	let IVAMonto = 0
	let IEPSMonto = 0

	const TipoAjusteId = 2   //CAMBIO DE PRESENTACION
	let AfectaCosto = ''

	const client = await pool.connect()

	try{
		await client.query('BEGIN')
		values=[SucursalId]
		sql=`SELECT COALESCE(MAX("FolioId"),0)+1 AS "FolioId" FROM ajustes_inventario WHERE "SucursalId" = $1`
		response = await client.query(sql,values)
		FolioId = response.rows[0].FolioId
		
		values=[TipoAjusteId]
		sql=`SELECT "AfectaCosto" FROM tipo_ajustes WHERE "TipoAjusteId" = $1`
		response = await client.query(sql,values)
		AfectaCosto = response.rows[0].AfectaCosto

//######################################################################################################################################################
//ACTUALIZA CAMBIO DE PRESENTACION "PADRE"
		values=[SucursalId,parseInt(CodigoIdPadre)]
		sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",ip."CostoPromedio",ip."PrecioVentaSinImpuesto",ip."PrecioVentaConImpuesto"
			FROM productos p INNER JOIN inventario_perpetuo ip ON p."CodigoId" = ip."CodigoId"
			WHERE ip."SucursalId" = $1
			AND p."CodigoId" = $2
			`
		response = await client.query(sql,values)
		CategoriaId = response.rows[0].CategoriaId
		SubcategoriaId = response.rows[0].SubcategoriaId
		UnidadesInventario = response.rows[0].UnidadesInventario
		UnidadesInventarioDespues = parseInt(UnidadesInventario) - parseInt(UnidadesConvertir)
		CostoCompra = parseFloat(response.rows[0].CostoCompra)
		CostoPromedio = parseFloat(response.rows[0].CostoPromedio)
		CostoPromedioPadre = parseFloat(response.rows[0].CostoPromedio)
		PrecioVentaSinImpuesto = parseFloat(response.rows[0].PrecioVentaSinImpuesto)
		PrecioVentaConImpuesto = parseFloat(response.rows[0].PrecioVentaConImpuesto)

		const vunidadesajustadasnegativas = parseInt(UnidadesConvertir) * -1

		values = [SucursalId,parseInt(CodigoIdPadre),FolioId,CodigoBarrasPadre,CategoriaId,SubcategoriaId,TipoAjusteId,AfectaCosto,vunidadesajustadasnegativas,UnidadesInventario,UnidadesInventarioDespues,CostoCompra,CostoPromedio,PrecioVentaSinImpuesto,PrecioVentaConImpuesto,parseInt(ColaboradorId),Usuario]

		sql = `INSERT INTO ajustes_inventario("SucursalId","CodigoId","FolioId","CodigoBarras","Fecha","CategoriaId","SubcategoriaId","TipoAjusteId",
			"AfectaCosto","UnidadesAjustadas","UnidadesInventarioAntes","UnidadesInventarioDespues","CostoCompra","CostoPromedio","PrecioVentaSinImpuesto",
			"PrecioVentaConImpuesto","ColaboradorId","FechaHora","Usuario") 
			VALUES($1,$2,$3,$4,CLOCK_TIMESTAMP(),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CLOCK_TIMESTAMP(),$17)
		`
		await client.query(sql,values)

		values = [SucursalId,parseInt(CodigoIdPadre),parseInt(UnidadesConvertir),Usuario]
		sql = `UPDATE inventario_perpetuo
			SET "UnidadesInventario" = "UnidadesInventario" - $3,
			"FechaUltimoAjuste" = CLOCK_TIMESTAMP(),
			"FechaHora" = CLOCK_TIMESTAMP(),
			"Usuario" = $4
			WHERE "SucursalId" = $1
			AND "CodigoId" = $2
		`
		await client.query(sql,values)


//######################################################################################################################################################
//ACTUALIZA CAMBIO DE PRESENTACION "HIJO"
		values=[SucursalId,parseInt(CodigoIdHijo)]
		sql=`SELECT p."CategoriaId",p."SubcategoriaId",ip."UnidadesInventario",ip."CostoCompra",ip."CostoPromedio",ip."PrecioVentaSinImpuesto",ip."PrecioVentaConImpuesto",
				ip."Margen",ip."IVA",ip."IEPS"
			FROM productos p INNER JOIN inventario_perpetuo ip ON p."CodigoId" = ip."CodigoId"
			WHERE ip."SucursalId" = $1
			AND p."CodigoId" = $2
			`
		response = await client.query(sql,values)
		CategoriaId = response.rows[0].CategoriaId
		SubcategoriaId = response.rows[0].SubcategoriaId
		UnidadesInventario = response.rows[0].UnidadesInventario
		UnidadesInventarioDespues = parseInt(UnidadesInventario) + parseInt(UnidadesHijoRecibe)
		CostoCompra = parseFloat(response.rows[0].CostoCompra)
		CostoPromedio = parseFloat(response.rows[0].CostoPromedio)
		PrecioVentaSinImpuesto = parseFloat(response.rows[0].PrecioVentaSinImpuesto)
		PrecioVentaConImpuesto = parseFloat(response.rows[0].PrecioVentaConImpuesto)
		Margen = parseFloat(response.rows[0].Margen)
		IVA = parseFloat(response.rows[0].IVA)
		IEPS = parseFloat(response.rows[0].IEPS)


		FolioId = FolioId + 1



		CostoPromedioPadre = CostoPromedioPadre / parseFloat(FactorConversion) 
		if(UnidadesInventario >= 0){
			NuevoCostoPromedio = ((parseInt(UnidadesHijoRecibe)*CostoPromedioPadre) + (UnidadesInventario*CostoPromedio))/(UnidadesHijoRecibe+UnidadesInventario)
		}else{
			NuevoCostoPromedio = ((parseInt(UnidadesHijoRecibe)*CostoPromedioPadre) + (0*CostoPromedio))/(UnidadesHijoRecibe+0)
		}

		if(PrecioVentaConImpuesto === 0){

			//Revisa si ese producto tiene Precio de Venta Con Impuesto en otra Sucursal para tomarlo de base
			values = [parseInt(CodigoIdHijo)]
			sql = `SELECT "PrecioVentaSinImpuesto","PrecioVentaConImpuesto"
				FROM inventario_perpetuo
				WHERE "CodigoId" = $1 
				AND "SucursalId" IN (SELECT "SucursalId" FROM sucursales WHERE "TipoSucursal" = 'S' AND "Status" = 'A')
				ORDER BY "PrecioVentaConImpuesto" DESC
				LIMIT 1
			`
			response = await client.query(sql,values)
			let PrecioVentaSinImpuesto2 = parseFloat(response.rows[0].PrecioVentaSinImpuesto)
			let PrecioVentaConImpuesto2 = parseFloat(response.rows[0].PrecioVentaConImpuesto)

			if(PrecioVentaConImpuesto2 > 0){ //Si otra sucursal tiene precio para ese producto toma como base ese Precio de Venta
				PrecioVentaSinImpuesto = PrecioVentaSinImpuesto2
				PrecioVentaConImpuesto = PrecioVentaConImpuesto2
			}else{
				PrecioVentaSinImpuesto = NuevoCostoPromedio / (1-(Margen/100))
				PrecioVentaConImpuesto = PrecioVentaSinImpuesto * (1+((IVA/100)+(IEPS/100)))


				PrecioVentaConImpuesto = Math.ceil(PrecioVentaConImpuesto)  //Nuevo Precio de Venta Con Impuesto Redondeado
				//PrecioVentaSinImpuesto = PrecioVentaConImpuesto / (1+((IVA/100)+(IEPS/100)))
				//IVAMonto = PrecioVentaSinImpuesto * (IVA/100)
				//IEPSMonto = PrecioVentaSinImpuesto * (IEPS/100)

				//MargenReal = (PrecioVentaSinImpuestos - NuevoCostoPromedio)/ PrecioVentaSinImpuesto
			}

		        values = [SucursalId,parseInt(CodigoIdHijo),FolioId,CodigoBarrasHijo,CategoriaId,SubcategoriaId,TipoAjusteId,AfectaCosto,parseInt(UnidadesHijoRecibe),UnidadesInventario,UnidadesInventarioDespues,CostoPromedioPadre,NuevoCostoPromedio,PrecioVentaSinImpuesto,PrecioVentaConImpuesto,parseInt(ColaboradorId),Usuario]

			sql = `INSERT INTO ajustes_inventario("SucursalId","CodigoId","FolioId","CodigoBarras","Fecha","CategoriaId","SubcategoriaId","TipoAjusteId",
			"AfectaCosto","UnidadesAjustadas","UnidadesInventarioAntes","UnidadesInventarioDespues","CostoCompra","CostoPromedio","PrecioVentaSinImpuesto",
			"PrecioVentaConImpuesto","ColaboradorId","FechaHora","Usuario") 
			VALUES($1,$2,$3,$4,CLOCK_TIMESTAMP(),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CLOCK_TIMESTAMP(),$17)
			`
			await client.query(sql,values)


			values = [SucursalId,parseInt(CodigoIdHijo),parseInt(UnidadesHijoRecibe),CostoPromedioPadre,NuevoCostoPromedio,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesInventario" = "UnidadesInventario" + $3,
				"CostoCompra" = $4,
				"CostoPromedio" = $5,
				"FechaUltimoAjuste" = CLOCK_TIMESTAMP(),
				"FechaHora" = CLOCK_TIMESTAMP(),
				"Usuario" = $6
			WHERE "SucursalId" = $1
			AND "CodigoId" = $2
			`
			await client.query(sql,values)


			//Actualiza el Precio de Venta Con Impuesto y el trigger tr_actualiza_precioventa actualiza el resto de los campos relacionados

			values = [parseInt(CodigoIdHijo),PrecioVentaConImpuesto]
			sql = `UPDATE inventario_perpetuo
				SET "PrecioVentaConImpuesto" = $2
				WHERE "CodigoId" = $1
			`

			await client.query(sql,values)
			
		}else{

		        values = [SucursalId,parseInt(CodigoIdHijo),FolioId,CodigoBarrasHijo,CategoriaId,SubcategoriaId,TipoAjusteId,AfectaCosto,parseInt(UnidadesHijoRecibe),UnidadesInventario,UnidadesInventarioDespues,CostoPromedioPadre,NuevoCostoPromedio,PrecioVentaSinImpuesto,PrecioVentaConImpuesto,parseInt(ColaboradorId),Usuario]

			sql = `INSERT INTO ajustes_inventario("SucursalId","CodigoId","FolioId","CodigoBarras","Fecha","CategoriaId","SubcategoriaId","TipoAjusteId",
			"AfectaCosto","UnidadesAjustadas","UnidadesInventarioAntes","UnidadesInventarioDespues","CostoCompra","CostoPromedio","PrecioVentaSinImpuesto",
			"PrecioVentaConImpuesto","ColaboradorId","FechaHora","Usuario") 
			VALUES($1,$2,$3,$4,CLOCK_TIMESTAMP(),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,CLOCK_TIMESTAMP(),$17)
			`
			await client.query(sql,values)



			MargenReal = (PrecioVentaSinImpuesto - NuevoCostoPromedio)/ PrecioVentaSinImpuesto * 100

			values = [SucursalId,parseInt(CodigoIdHijo),parseInt(UnidadesHijoRecibe),CostoPromedioPadre,NuevoCostoPromedio,MargenReal,Usuario]
			sql = `UPDATE inventario_perpetuo
				SET "UnidadesInventario" = "UnidadesInventario" + $3,
				"CostoCompra" = $4,
				"CostoPromedio" = $5,
				"MargenReal" = $6,
				"FechaUltimoAjuste" = CLOCK_TIMESTAMP(),
				"FechaHora" = CLOCK_TIMESTAMP(),
				"Usuario" = $7
			WHERE "SucursalId" = $1
			AND "CodigoId" = $2
		`
		await client.query(sql,values)

		}

//######################################################################################################################################################


		await client.query('COMMIT')
		res.status(200).json({"message":"Success!!!"})

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}finally{
		client.release()
	}
})

app.get('/api/consultaproductosinventarioperpetuo/:SucursalId/:CodigoId',authenticationToken,async(req,res) => {
	const SucursalId = req.params.SucursalId
	const CodigoId = req.params.CodigoId

	try{
		let values = [SucursalId,CodigoId]

		let sql = `SELECT c."Categoria",sc."Subcategoria",
				iiva."Descripcion" AS "IVADescripcion",
				p."IVACompra" AS "IVACompraProductos",p."IEPSId" AS "IEPSIdProductos",
				p."PadreHijo",p."Hermano",p."Inventariable",
				p."CompraVenta",p."ComisionVentaPorcentaje",p."Status" AS "StatusProductos",
				to_char(p."FechaHora", 'yyyy-MM-dd HH24:MI:ss.ms.us') AS "FechaHoraProductos",
				p."Usuario" AS "UsuarioProductos",
				ip."UnidadesInventario",ip."UnidadesTransito",ip."UnidadesComprometidas",ip."CostoCompra",ip."CostoPromedio",
				ip."Margen",CAST(ip."MargenReal" AS DEC(5,2)),ip."PrecioVentaSinImpuesto",ip."IVAId" AS "IVAIdInventario",ip."IVA" AS "IVAInventario",
				ip."IVAMonto" AS "IVAMontoInventario",ip."IEPSId" AS "IEPSIdInventario",ip."IEPS" AS "IEPSInventario",ip."IEPSMonto" AS "IEPSMontoInventario",
				ip."PrecioVentaConImpuesto",
				ip."Maximo",ip."Minimo",ip."FechaCambioPrecio",ip."FechaUltimaVenta",ip."FechaUltimaCompra",ip."FechaUltimoTraspasoSalida",
				ip."FechaUltimoTraspasoEntrada",ip."FechaUltimoAjuste",ip."Status" AS "StatusInventario",
				to_char(ip."FechaHora", 'yyyy-MM-dd HH24:MI:ss.ms.us') AS "FechaHoraInventario",
				ip."Usuario" AS "UsuarioInventario"
				FROM productos p
				INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = p."CodigoId"
				INNER JOIN categorias c ON c."CategoriaId" = p."CategoriaId"
				INNER JOIN subcategorias sc ON sc."CategoriaId" = p."CategoriaId" AND sc."SubcategoriaId" = p."SubcategoriaId"
				INNER JOIN impuestos_iva iiva ON iiva."IVAId" = p."IVAId"
				WHERE ip."SucursalId" = $1 AND p."CodigoId" = $2
		`
		let response = await pool.query(sql,values)
		let data;

		if(response.rowCount === 1){
			data = response.rows
		}else{
			data = {"message": "Producto No Existe"}
		}

		values = [CodigoId]
		sql =`SELECT s."Sucursal",ip."UnidadesInventario" - ip."UnidadesComprometidas" AS "UnidadesDisponibles"
				FROM sucursales s
				INNER JOIN inventario_perpetuo ip ON ip."SucursalId" = s."SucursalId"
				WHERE ip."CodigoId" = $1
			ORDER BY s."SucursalId"
			`
		response = await pool.query(sql,values)

		let data2 = response.rows

		data.push(data2)

		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/estadoderesultadoslimpiaduria/:Periodo',authenticationToken,async(req,res) =>{
	const Periodo = req.params.Periodo 

	let values=[Periodo]
	let sql = `SELECT  DISTINCT "PrimerDiaMes","UltimoDiaMes"
				FROM dim_catalogo_tiempo
				WHERE "Periodo" = $1 
	`
	let response = await pool.query(sql,values)
	let data = response.rows 
	const PrimerDiaMes = data[0].PrimerDiaMes 
	const UltimoDiaMes = data[0].UltimoDiaMes


	try{
		values = [PrimerDiaMes,UltimoDiaMes]
		sql = `
		SELECT
		'Ventas' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 1
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc02"
		FROM registro_contable rc
		WHERE "SucursalId" = 2
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc03"
		FROM registro_contable rc
		WHERE "SucursalId" = 3
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc04"
		FROM registro_contable rc
		WHERE "SucursalId" = 4
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" = 1000
		),
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'Gastos Sucursal' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 1
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc02"
		FROM registro_contable rc
		WHERE "SucursalId" = 2
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc03"
		FROM registro_contable rc
		WHERE "SucursalId" = 3
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc04"
		FROM registro_contable rc
		WHERE "SucursalId" = 4
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 1
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT 
		'** UTILIDAD BRUTA' AS "Concepto",
		0 AS "Suc01",
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'Gastos Planta Matriz' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 100
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 10
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT 
		'** UTILIDAD DE OPERACION' AS "Concepto",
		0 AS "Suc01",
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'Melate' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 1
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc02"
		FROM registro_contable rc
		WHERE "SucursalId" = 2
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc03"
		FROM registro_contable rc
		WHERE "SucursalId" = 3
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 1000
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc04"
		FROM registro_contable rc
		WHERE "SucursalId" = 4
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 1000
		),
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'Melate Pagos' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 1
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '001'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc02"
		FROM registro_contable rc
		WHERE "SucursalId" = 2
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '001'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc03"
		FROM registro_contable rc
		WHERE "SucursalId" = 3
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '001'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc04"
		FROM registro_contable rc
		WHERE "SucursalId" = 4
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '001'
		),
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'Melate Pagos Clientes' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 1
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '002'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc02"
		FROM registro_contable rc
		WHERE "SucursalId" = 2
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '002'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc03"
		FROM registro_contable rc
		WHERE "SucursalId" = 3
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '002'
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc04"
		FROM registro_contable rc
		WHERE "SucursalId" = 4
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 2
		AND "CuentaContableId" = 13000
		AND "SubcuentaContableId" = '002'
		),
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT 
		'** UTILIDAD MELATE' AS "Concepto",
		0 AS "Suc01",
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'RENTAS EMPRESA' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 101
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 11
		AND "CuentaContableId" = 1001
		AND "SubcuentaContableId" = '001'
		),
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'OTROS INGRESOS EMPRESA' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 101
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 11
		AND "CuentaContableId" = 1002
		AND "SubcuentaContableId" = '001'
		),
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT
		'OTROS GASTOS EMPRESA' AS "Concepto",
		(SELECT COALESCE(SUM("Monto"),0) AS "Suc01"
		FROM registro_contable rc
		WHERE "SucursalId" = 101
		AND "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" = 11
		AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)
		),
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		UNION ALL
		SELECT 
		'** UAIR' AS "Concepto",
		0 AS "Suc01",
		0 AS "Suc02",
		0 AS "Suc03",
		0 AS "Suc04",
		0 AS "Suc05",
		0 AS "Suc06",
		0 AS "Total"
		`
		response = await pool.query(sql,values)
		data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}

})

app.get('/api/estadoderesultadoslimpiaduriacifracontrol/:Periodo',authenticationToken,async(req,res) =>{
	const Periodo = req.params.Periodo 

	let values=[Periodo]
	let sql = `SELECT  DISTINCT "PrimerDiaMes","UltimoDiaMes"
				FROM dim_catalogo_tiempo
				WHERE "Periodo" = $1 
	`
	let response = await pool.query(sql,values)
	let data = response.rows 
	const PrimerDiaMes = data[0].PrimerDiaMes 
	const UltimoDiaMes = data[0].UltimoDiaMes


	try{
		values = [PrimerDiaMes,UltimoDiaMes]
		sql = `
		SELECT COALESCE(SUM("Monto"),0) AS "Monto"
		FROM registro_contable rc
		WHERE "Fecha" BETWEEN $1 AND $2
		AND "UnidadDeNegocioId" NOT IN (3,4,5)
		`
		response = await pool.query(sql,values)
		data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}

})

app.get('/api/consultaaniosactivos',authenticationToken,async(req,res) =>{
	const sql = `SELECT DISTINCT EXTRACT(YEAR FROM "Fecha") AS "Year"
				FROM registro_contable
				ORDER BY 1 DESC
	`
	try{
		const response = await pool.query(sql)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultaperiodosporanio/:anio',authenticationToken,async(req,res) =>{
	const anio = req.params.anio

	const values = [anio]
	const sql = `SELECT DISTINCT "Periodo" AS "Periodo"
				FROM dim_catalogo_tiempo
				WHERE "Año" = $1
				ORDER BY 1 
	`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultalimpiaduriaventaspormes/:anio',authenticationToken,async(req,res) =>{
	const anio = req.params.anio

	const values = [anio]
	const sql = `SELECT EXTRACT(MONTH FROM dim."Fecha") AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM dim_catalogo_tiempo dim 
				LEFT JOIN registro_contable rc ON rc."Fecha" = dim."Fecha" 
								AND "UnidadDeNegocioId" = 1 AND "CuentaContableId" = 1000
				WHERE dim."Año" = $1
				GROUP BY EXTRACT(MONTH FROM dim."Fecha")
				UNION ALL
				SELECT 13 AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = $1
				AND "UnidadDeNegocioId" = 1 AND "CuentaContableId" = 1000
				GROUP BY "Mes"
				ORDER BY 1 ;
	`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultalimpiaduriaegresospormes/:anio',authenticationToken,async(req,res) =>{
	const anio = req.params.anio

	const values = [anio]
	const sql = `SELECT EXTRACT(MONTH FROM dim."Fecha") AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM dim_catalogo_tiempo dim 
				LEFT JOIN registro_contable rc ON rc."Fecha" = dim."Fecha" 
								AND "UnidadDeNegocioId" IN (1,10,11) 
								AND "CuentaContableId" NOT BETWEEN 1000 AND 1999
				WHERE dim."Año" = $1
				GROUP BY EXTRACT(MONTH FROM dim."Fecha")
				UNION ALL
				SELECT 13 AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = $1
				AND "UnidadDeNegocioId" IN (1,10,11)
				AND "CuentaContableId" NOT BETWEEN 1000 AND 1999
				GROUP BY "Mes"
				ORDER BY 1 ;
	`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})
app.get('/api/consultamelateventaspormes/:anio',authenticationToken,async(req,res) =>{
	const anio = req.params.anio

	const values = [anio]
	const sql = `SELECT EXTRACT(MONTH FROM dim."Fecha") AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM dim_catalogo_tiempo dim 
				LEFT JOIN registro_contable rc ON rc."Fecha" = dim."Fecha" 
								AND "UnidadDeNegocioId" = 2 AND "CuentaContableId" = 1000
				WHERE dim."Año" = $1
				GROUP BY EXTRACT(MONTH FROM dim."Fecha")
				UNION ALL
				SELECT 13 AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = $1
				AND "UnidadDeNegocioId" = 2 AND "CuentaContableId" = 1000
				GROUP BY "Mes"
				ORDER BY 1 ;
	`
	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultamelateegresospormes/:anio',authenticationToken,async(req,res) =>{
	const anio = req.params.anio

	const values = [anio]
	const sql = `SELECT EXTRACT(MONTH FROM dim."Fecha") AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM dim_catalogo_tiempo dim 
				LEFT JOIN registro_contable rc ON rc."Fecha" = dim."Fecha" 
								AND "UnidadDeNegocioId" IN (2) 
								AND "CuentaContableId" NOT BETWEEN 1000 AND 1999
				WHERE dim."Año" = $1
				GROUP BY EXTRACT(MONTH FROM dim."Fecha")
				UNION ALL
				SELECT 13 AS "Mes",
				COALESCE(SUM("Monto"),0) AS "Monto"
				FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = $1
				AND "UnidadDeNegocioId" IN (2)
				AND "CuentaContableId" NOT BETWEEN 1000 AND 1999
				GROUP BY "Mes"
				ORDER BY 1 ;
	`


	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/api/gastosinversionesporanio/:year',authenticationToken,async (req,res) =>{
	const year = req.params.year
	const values = [year]

	try{
		const sql = `
		SELECT 0 AS id, cc."CuentaContableId",cc."CuentaContable",scc."SubcuentaContable",
		(SELECT COALESCE(SUM("Monto"),0) AS "Ene"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 1
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Feb"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 2
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Mar"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 3
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Abr"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 4
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "May"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 5
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Jun"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 6
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Jul"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 7
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Ago"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 8
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Sep"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 9
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Oct"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 10
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Nov"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 11
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Dic"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(MONTH FROM "Fecha") = 12
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		(SELECT COALESCE(SUM("Monto"),0) AS "Total"
		FROM registro_contable rc
		WHERE rc."CuentaContableId" = scc."CuentaContableId"
		AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		AND rc."UnidadDeNegocioId" IN (1,10,11) 
		AND EXTRACT(YEAR FROM "Fecha") = $1
		),
		0 AS "PorcentajeSimple"

		FROM cuentas_contables cc
		INNER JOIN subcuentas_contables scc ON scc."CuentaContableId" = cc."CuentaContableId"
		WHERE cc."CuentaContableId" >= 2000
		ORDER BY cc."CuentaContableId",scc."SubcuentaContable"
		`

		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultagastosinversionperiodo/:Periodo',authenticationToken,async(req,res) =>{
	const Periodo = req.params.Periodo 

	
	try{
		const values = [Periodo]
		
		const sql = `SELECT 0 AS "Posicion", rc."SucursalId",s."Sucursal",udn."UnidadDeNegocio",rc."CuentaContableId","CuentaContable","SubcuentaContable",rc."Comentarios",rc."Monto",CAST (rc."FechaHora" AS CHAR(16)),rc."Usuario"
		FROM registro_contable rc
		INNER JOIN cuentas_contables cc ON cc."CuentaContableId" = rc."CuentaContableId"
		INNER JOIN subcuentas_contables scc ON scc."CuentaContableId" = rc."CuentaContableId" AND scc."SubcuentaContableId" = rc."SubcuentaContableId"
		INNER JOIN sucursales s ON s."SucursalId" = rc."SucursalId"
		INNER JOIN unidades_de_negocio udn ON udn."UnidadDeNegocioId" = rc."UnidadDeNegocioId"
		WHERE rc."UnidadDeNegocioId" IN (1,2,10,11)
		AND "Periodo" = $1
		ORDER BY rc."FechaHora"`

		const response = await pool.query(sql,values)
		const data = response.rows

		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ventas/bi/lavamatica/:year',authenticationToken,async(req,res) =>{
	const ejercicio = req.params.year

	try{
		const values = [ejercicio]
		const sql = `SELECT 1 AS "Numero",'VentasConImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3 AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Mes"
			UNION ALL
			SELECT 2 AS "Numero",'Impuestos' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto" - "UnidadesVendidas" * "PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 3 AS "Numero",'VentasSinImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 4 AS "Numero",'CostoPromedio' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
			COALESCE(SUM("Monto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
					AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
					AND rc."UnidadDeNegocioId" = 3  AND EXTRACT(YEAR FROM rc."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"




			UNION ALL
			SELECT 7 AS "Numero",'ComisionesVenta' AS "Transaccion","Mes",
			COALESCE(ROUND(SUM(v."UnidadesVendidas"*v."ComisionVenta")),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3 AND EXTRACT(YEAR FROM v."Fecha") = $1
					AND v."CodigoId" = 65 
			GROUP BY "Mes"





			UNION ALL
			SELECT 8 AS "Numero",'UtilidadNeta'AS "Transaccion",u."Mes",SUM(u."Monto"+e."Monto"-f."Monto") FROM (
				SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
						COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS u
				INNER JOIN
				(SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
						COALESCE(SUM("Monto"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
						AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
						AND rc."UnidadDeNegocioId" = 3  AND EXTRACT(YEAR FROM rc."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS e ON u."Mes" = e."Mes"


				INNER JOIN
				(SELECT 7 AS "Numero",'ComisionesVenta' AS "Transaccion","Mes",
						COALESCE(SUM(v."UnidadesVendidas"*v."ComisionVenta"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 3  AND EXTRACT(YEAR FROM v."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS f ON u."Mes" = f."Mes"




				GROUP BY u."Numero",u."Transaccion",u."Mes"



			ORDER BY "Numero","Transaccion","Mes"
		`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/ventas/bi/tienda/:year',authenticationToken,async(req,res) =>{
	const ejercicio = req.params.year

	try{
		const values = [ejercicio]
		const sql = `SELECT 1 AS "Numero",'VentasConImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3) AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Mes"
			UNION ALL
			SELECT 2 AS "Numero",'Impuestos' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto" - "UnidadesVendidas" * "PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3)  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 3 AS "Numero",'VentasSinImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3)  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 4 AS "Numero",'CostoPromedio' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3) AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3)  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
			COALESCE(SUM("Monto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
					AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
					AND rc."UnidadDeNegocioId" = 5  AND EXTRACT(YEAR FROM rc."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"

			UNION ALL
			SELECT 7 AS "Numero",'UtilidadNeta'AS "Transaccion",u."Mes",SUM(u."Monto"+e."Monto") FROM (
				SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
						COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" NOT IN (1,3) AND EXTRACT(YEAR FROM v."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS u
				INNER JOIN
				(SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
						COALESCE(SUM("Monto"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
						AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
						AND rc."UnidadDeNegocioId" = 5  AND EXTRACT(YEAR FROM rc."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS e ON u."Mes" = e."Mes"
				GROUP BY u."Numero",u."Transaccion",u."Mes"



			ORDER BY "Numero","Transaccion","Mes"
		`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/api/ventas/bi/decorafiestas/:year',authenticationToken,async(req,res) =>{
	const ejercicio = req.params.year

	try{
		const values = [ejercicio]
		const sql = `SELECT 1 AS "Numero",'VentasConImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1 AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Mes"
			UNION ALL
			SELECT 2 AS "Numero",'Impuestos' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaConImpuesto" - "UnidadesVendidas" * "PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 3 AS "Numero",'VentasSinImpuesto' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 4 AS "Numero",'CostoPromedio' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1 AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
			COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1  AND EXTRACT(YEAR FROM v."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"
			UNION ALL
			SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
			COALESCE(SUM("Monto"),0) AS "Monto"
			FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
					LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
					AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
					AND rc."UnidadDeNegocioId" = 4  AND EXTRACT(YEAR FROM rc."Fecha") = $1
			GROUP BY "Numero","Transaccion","Mes"

			UNION ALL
			SELECT 7 AS "Numero",'UtilidadNeta'AS "Transaccion",u."Mes",SUM(u."Monto"+e."Monto") FROM (
				SELECT 5 AS "Numero",'UtilidadBruta' AS "Transaccion","Mes",
						COALESCE(SUM("UnidadesVendidas"*"PrecioVentaSinImpuesto" - "UnidadesVendidas"*"CostoPromedio"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN ventas v ON m."Mes" = EXTRACT(MONTH FROM v."Fecha") AND v."CategoriaId" = 1 AND EXTRACT(YEAR FROM v."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS u
				INNER JOIN
				(SELECT 6 AS "Numero",'Egresos' AS "Transaccion","Mes",
						COALESCE(SUM("Monto"),0) AS "Monto"
						FROM (SELECT DISTINCT "Mes" FROM dim_catalogo_tiempo) AS m
						LEFT JOIN registro_contable rc ON m."Mes" = EXTRACT(MONTH FROM rc."Fecha")
						AND rc."CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" =-1)
						AND rc."UnidadDeNegocioId" = 4  AND EXTRACT(YEAR FROM rc."Fecha") = $1
				GROUP BY "Numero","Transaccion","Mes") AS e ON u."Mes" = e."Mes"
				GROUP BY u."Numero",u."Transaccion",u."Mes"



			ORDER BY "Numero","Transaccion","Mes"
		`
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)

	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/limpiaduria/bi/estadoresultadoslimpiadurianegocios/:year',authenticationToken,async(req,res) =>{
	const year = parseInt(req.params.year)
	try{
		const values = [year]
		const sql = `SELECT 'Limpiaduria Utilidad' AS "Negocio",dim."Año",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 1 AND "UnidadDeNegocioId" IN (1,10)) "Ene",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 2 AND "UnidadDeNegocioId" IN (1,10)) "Feb",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 3 AND "UnidadDeNegocioId" IN (1,10)) "Mar",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 4 AND "UnidadDeNegocioId" IN (1,10)) "Abr",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 5 AND "UnidadDeNegocioId" IN (1,10)) "May",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 6 AND "UnidadDeNegocioId" IN (1,10)) "Jun",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 7 AND "UnidadDeNegocioId" IN (1,10)) "Jul",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 8 AND "UnidadDeNegocioId" IN (1,10)) "Ago",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 9 AND "UnidadDeNegocioId" IN (1,10)) "Sep",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 10 AND "UnidadDeNegocioId" IN (1,10)) "Oct",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 11 AND "UnidadDeNegocioId" IN (1,10)) "Nov",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 12 AND "UnidadDeNegocioId" IN (1,10)) "Dic",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") < 13 AND "UnidadDeNegocioId" IN (1,10)) "Total"
		FROM dim_catalogo_tiempo dim
		WHERE dim."Año" = $1
		GROUP BY "Negocio",dim."Año"
		UNION ALL
		SELECT 'Melate Utilidad' AS "Negocio",dim."Año",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 1 AND "UnidadDeNegocioId" IN (2)) "Ene",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 2 AND "UnidadDeNegocioId" IN (2)) "Feb",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 3 AND "UnidadDeNegocioId" IN (2)) "Mar",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 4 AND "UnidadDeNegocioId" IN (2)) "Abr",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 5 AND "UnidadDeNegocioId" IN (2)) "May",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 6 AND "UnidadDeNegocioId" IN (2)) "Jun",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 7 AND "UnidadDeNegocioId" IN (2)) "Jul",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 8 AND "UnidadDeNegocioId" IN (2)) "Ago",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 9 AND "UnidadDeNegocioId" IN (2)) "Sep",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 10 AND "UnidadDeNegocioId" IN (2)) "Oct",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 11 AND "UnidadDeNegocioId" IN (2)) "Nov",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 12 AND "UnidadDeNegocioId" IN (2)) "Dic",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") < 13 AND "UnidadDeNegocioId" IN (2)) "Total"
		FROM dim_catalogo_tiempo dim
		WHERE dim."Año" = $1
		GROUP BY "Negocio",dim."Año"
		UNION ALL
		SELECT 'Rentas' AS "Negocio",dim."Año",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 1 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Ene",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 2 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Feb",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 3 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Mar",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 4 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Abr",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 5 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "May",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 6 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Jun",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 7 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Jul",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 8 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Ago",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 9 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Sep",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 10 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Oct",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 11 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Nov",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 12 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Dic",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") < 13 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1001) "Total"
		FROM dim_catalogo_tiempo dim
		WHERE dim."Año" = $1
		GROUP BY "Negocio",dim."Año"
		UNION ALL
		SELECT 'Otros Ingresos' AS "Negocio",dim."Año",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 1 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Ene",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 2 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Feb",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 3 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Mar",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 4 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Abr",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 5 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "May",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 6 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Jun",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 7 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Jul",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 8 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Ago",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 9 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Sep",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 10 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Oct",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 11 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Nov",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 12 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Dic",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") < 13 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" = 1002) "Total"
		FROM dim_catalogo_tiempo dim
		WHERE dim."Año" = $1
		GROUP BY "Negocio",dim."Año"
		UNION ALL
		SELECT 'Otros Gastos' AS "Negocio",dim."Año",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 1 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Ene",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 2 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Feb",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 3 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Mar",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 4 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Abr",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 5 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "May",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 6 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Jun",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 7 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Jul",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 8 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Ago",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 9 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Sep",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 10 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Oct",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 11 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Nov",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") = 12 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Dic",
		(SELECT COALESCE(SUM("Monto"),0) FROM registro_contable rc WHERE EXTRACT(YEAR FROM rc."Fecha") = dim."Año" AND EXTRACT(MONTH FROM "Fecha") < 13 AND "UnidadDeNegocioId" IN (11) AND "CuentaContableId" IN (SELECT "CuentaContableId" FROM cuentas_contables WHERE "NaturalezaCC" = -1)) "Total"
		FROM dim_catalogo_tiempo dim
		WHERE dim."Año" = $1
		GROUP BY "Negocio",dim."Año"
		`

		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/consultainvnetarioperpetuohistoriaporperiodo/:year',authenticationToken,async(req, res) =>{
	const year = req.params.year 
	const values = [year]
	try{
		const sql = `SELECT 'Inventario Perpetuo' AS "Transaccion",
		
		            (SELECT 
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Ene"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 1
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Feb"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 2
					),
					
					(SELECT 
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Mar"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 3
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Abr"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 4
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "May"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 5
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Jun"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 6
					),
					
					(SELECT 
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Jul"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 7
					),
					
					(SELECT 
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Ago"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 8
					),
					
					(SELECT 
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Sep"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 9
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Oct"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 10
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Nov"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 11
					),
					
					(SELECT
					COALESCE(SUM("UnidadesInventario"*"CostoPromedio"),0) AS "Dic"
					FROM inventario_perpetuo_historia iph 
					INNER JOIN dim_catalogo_tiempo ct ON ct."Fecha" = iph."FechaRespaldo" 
					WHERE ct."Año" = $1
					AND ct."Mes" = 12
					)
					`
		const response = await pool.query(sql,values)
		const data = response.rows 
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/inventariofaltantes/:SucursalId',authenticationToken,async(req, res) => {
	const SucursalId = parseInt(req.params.SucursalId)
	const values = [SucursalId] 
	const sql = `
		SELECT ip."SucursalId",
		vw."CodigoId",
		vw."Descripcion",
		ip."Maximo",
		ip."Minimo",
		ip."UnidadesInventario" AS "UniInv",
		COALESCE(SUM(CAST(v."UnidadesVendidas" AS INT)),0) AS "UnidadesDesplazadas",
		(SELECT ip2."UnidadesInventario" FROM inventario_perpetuo ip2 WHERE ip2."CodigoId" = vw."CodigoId" AND ip2."SucursalId" = 99) AS "UniInvCedis"
		FROM vw_productos_descripcion vw
		INNER JOIN inventario_perpetuo ip  ON vw."CodigoId" = ip."CodigoId"
		INNER JOIN productos p ON vw."CodigoId" = p."CodigoId"
		LEFT JOIN ventas v ON v."SucursalId" = ip."SucursalId" AND v."CodigoId" = ip."CodigoId" AND v."Fecha" >= CURRENT_DATE - 35 AND v."Status" = 'V'
		WHERE ip."UnidadesInventario" < ip."Minimo"
		AND ip."Minimo" > 0
		AND p."CategoriaId" <> 1
		AND (ip."FechaUltimaCompra" IS NOT NULL OR ip."FechaUltimoTraspasoEntrada" IS NOT NULL OR ip."FechaUltimoAjuste" IS NOT NULL)
		AND ip."SucursalId" IN (SELECT "SucursalId" FROM sucursales WHERE "TipoSucursal" IN ('S','C') AND "SucursalId" = $1 )
		GROUP BY ip."SucursalId",vw."CodigoId",vw."Descripcion",p."CategoriaId",ip."Maximo",ip."Minimo",ip."UnidadesInventario"
		ORDER BY ip."SucursalId",p."CategoriaId", ip."UnidadesInventario" DESC
		`
		try{
			const response = await pool.query(sql,values)
			const data = response.rows
			res.status(200).json(data)
		}catch(error){
			console.log(error.message)
			res.status(500).json({"error": error.message})
		}
})

app.get('/api/lavadassecadasservicios/:SucursalId/:year',authenticationToken,async(req, res) => {
		const SucursalId = req.params.SucursalId
		let SucursalIdIni = 0
		let SucursalIdFin = 0

		if (SucursalId == 0){
			SucursalIdIni = 1
			SucursalIdFin = 1000
		}else{
			SucursalIdIni = SucursalId
			SucursalIdFin = SucursalId
		}

		const year = req.params.year
		const values = [SucursalIdIni,SucursalIdFin,year]
		
		const sql = `SELECT vw."CodigoId" ||' '|| vw."Descripcion" AS "Descripcion",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 1 ) AS "Ene",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 2 ) AS "Feb",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 3 ) AS "Mar",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 4 ) AS "Abr",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 5 ) AS "May",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 6 ) AS "Jun",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 7 ) AS "Jul",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 8 ) AS "Ago",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 9 ) AS "Sep",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 10 ) AS "Oct",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 11 ) AS "Nov",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 AND EXTRACT(MONTH FROM v."Fecha") = 12 ) AS "Dic",
		(SELECT COALESCE(SUM(v."UnidadesVendidas"),0) FROM ventas v WHERE v."SucursalId" BETWEEN $1 AND $2 AND v."CodigoId" = vw."CodigoId"
				AND EXTRACT(YEAR FROM v."Fecha") = $3 ) AS "Total"
		FROM vw_productos_descripcion vw
		WHERE vw."CodigoId" IN (63,64,65)
		ORDER BY "Descripcion"
		`
		try{
			const response = await pool.query(sql,values)
			const data = response.rows
			res.status(200).json(data)
		}catch(error){
			console.log(error.message)
			res.status(500).json({"error": error.message})
		}
})

app.get('/api/egresoslimpiaduriacuentacontable/:year/:consulta',authenticationToken,async(req, res) =>{
	const fechaInicial= `01/01/${req.params.year}`
	const fechaFinal= `12/31/${req.params.year}`
	const consulta = req.params.consulta
	const values = [fechaInicial,fechaFinal]
	let sql=``
	if (consulta === 'cuenta'){
	sql = `
			SELECT cc."CuentaContableId",cc."CuentaContable",EXTRACT(MONTH FROM rc."Fecha") AS "Mes", ROUND(SUM("Monto")) AS "Monto"
			FROM registro_contable rc
			INNER JOIN cuentas_contables cc ON cc."CuentaContableId" = rc."CuentaContableId"
			WHERE "Fecha" between $1 and $2
			AND "UnidadDeNegocioId" IN (1,2,10,11)
			AND cc."NaturalezaCC" = -1
			AND cc."CuentaContableId" != 13000 
			GROUP BY cc."CuentaContableId",cc."CuentaContable","Mes"
			ORDER BY cc."CuentaContableId",cc."CuentaContable"
			`
	}
	if (consulta === 'subcuenta'){
	sql = `
			SELECT cc."CuentaContableId",cc."CuentaContable",scc."SubcuentaContableId",scc."SubcuentaContable",
			EXTRACT(MONTH FROM rc."Fecha") AS "Mes", ROUND(SUM("Monto")) AS "Monto"
			FROM registro_contable rc
			INNER JOIN cuentas_contables cc ON cc."CuentaContableId" = rc."CuentaContableId"
			INNER JOIN subcuentas_contables scc ON scc."CuentaContableId" = rc."CuentaContableId" AND scc."SubcuentaContableId" = rc."SubcuentaContableId"
			WHERE "Fecha" between $1 and $2
			AND "UnidadDeNegocioId" IN (1,2,10,11)
			AND cc."NaturalezaCC" = -1
			AND cc."CuentaContableId" != 13000 
			GROUP BY cc."CuentaContableId",cc."CuentaContable",scc."SubcuentaContableId",scc."SubcuentaContable","Mes"
			ORDER BY cc."CuentaContableId",scc."SubcuentaContableId"
			`
	}




	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/egresoslimpiaduriacuentacontablesubcuentacontablemes/:year/:mes/:CuentaContableId/:SubcuentaContableId/:TipoConsulta',authenticationToken,async(req, res) =>{
	const year = req.params.year
	const mes = req.params.mes
	const CuentaContableId = req.params.CuentaContableId
	const SubcuentaContableId = req.params.SubcuentaContableId
	const TipoConsulta = req.params.TipoConsulta


	let values = []
	let sql=``
	if (TipoConsulta === 'cuenta'){
		values = [year,mes,CuentaContableId]
		sql = `
			SELECT cc."CuentaContableId",cc."CuentaContable",scc."SubcuentaContableId",scc."SubcuentaContable",
			"Monto",rc."Comentarios"
			FROM registro_contable rc
			INNER JOIN cuentas_contables cc ON cc."CuentaContableId" = rc."CuentaContableId"
			INNER JOIN subcuentas_contables scc ON scc."CuentaContableId" = rc."CuentaContableId" AND scc."SubcuentaContableId" = rc."SubcuentaContableId"
			WHERE EXTRACT(YEAR FROM "Fecha") = $1 
			AND EXTRACT(MONTH FROM "Fecha") = $2
			AND cc."CuentaContableId" = $3
			AND "UnidadDeNegocioId" IN (1,2,10,11)
			AND cc."NaturalezaCC" = -1
			AND cc."CuentaContableId" != 13000 
			ORDER BY cc."CuentaContableId",scc."SubcuentaContableId"
			`
	}
	if (TipoConsulta === 'subcuenta'){
		values = [year,mes,CuentaContableId,SubcuentaContableId]
		sql = `
			SELECT cc."CuentaContableId",cc."CuentaContable",scc."SubcuentaContableId",scc."SubcuentaContable",
			"Monto",rc."Comentarios"
			FROM registro_contable rc
			INNER JOIN cuentas_contables cc ON cc."CuentaContableId" = rc."CuentaContableId"
			INNER JOIN subcuentas_contables scc ON scc."CuentaContableId" = rc."CuentaContableId" AND scc."SubcuentaContableId" = rc."SubcuentaContableId"
			WHERE EXTRACT(YEAR FROM "Fecha") = $1 
			AND EXTRACT(MONTH FROM "Fecha") = $2
			AND cc."CuentaContableId" = $3
			AND scc."SubcuentaContableId" = $4
			AND "UnidadDeNegocioId" IN (1,2,10,11)
			AND cc."NaturalezaCC" = -1
			AND cc."CuentaContableId" != 13000 
			ORDER BY cc."CuentaContableId",scc."SubcuentaContableId"
			`
	}

	try{
		const response = await pool.query(sql,values)
		const data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})

app.get('/api/productosmasdesplazadosmargen/:FechaInicial/:FechaFinal/:SucursalId',authenticationToken,async(req, res)=>{
	const fechaInicial = req.params.FechaInicial
	const fechaFinal = req.params.FechaFinal
	const SucursalId = req.params.SucursalId

	let values = [fechaInicial,fechaFinal,SucursalId]
	let sql = `SELECT vw."CodigoId",CAST(vw."Descripcion" AS VARCHAR(80)),
				SUM(v."UnidadesVendidas") AS "UnidadesVendidas",SUM(v."UnidadesVendidas"*v."PrecioVentaConImpuesto") AS "ExtVtaCImp",
				ip."PrecioVentaConImpuesto",
				CAST(ip."MargenReal" AS DEC(5,2)) AS "MargenActual",
				
				(SELECT CAST (MIN((1-("CostoPromedio"/"PrecioVentaSinImpuesto"))*100) AS DEC(5,2)) FROM ventas v
				WHERE v."CodigoId" = vw."CodigoId" AND v."Status" = 'V' `
				if(SucursalId != 0){
					sql+=`AND v."SucursalId" = $3 `
				}
				sql+=`AND EXTRACT(YEAR FROM v."Fecha") = EXTRACT(YEAR FROM CURRENT_DATE)) AS "MargenMinAño",
				
				(SELECT CAST (MAX((1-("CostoPromedio"/"PrecioVentaSinImpuesto"))*100) AS DEC(5,2)) FROM ventas v
				WHERE v."CodigoId" = vw."CodigoId" AND v."Status" = 'V' `
				if(SucursalId != 0){
					sql+=`AND v."SucursalId" = $3 `
				}
				sql+=`AND EXTRACT(YEAR FROM v."Fecha") = EXTRACT(YEAR FROM CURRENT_DATE)) AS "MargenMaxAño",
				
				(SELECT CAST (AVG((1-("CostoPromedio"/"PrecioVentaSinImpuesto"))*100) AS DEC(5,2)) FROM ventas v
				WHERE v."CodigoId" = vw."CodigoId" AND v."Status" = 'V' `
				if(SucursalId != 0){
					sql+=`AND v."SucursalId" = $3 `
				}
				sql+=`AND EXTRACT(YEAR FROM v."Fecha") = EXTRACT(YEAR FROM CURRENT_DATE)) AS "MargenAño",

				(SELECT CAST (AVG((1-("CostoPromedio"/"PrecioVentaSinImpuesto"))*100) AS DEC(5,2)) FROM ventas v
				WHERE v."CodigoId" = vw."CodigoId" AND v."Status" = 'V' `
				if(SucursalId != 0){
					sql+=`AND v."SucursalId" = $3 `
				}
				sql+=`AND EXTRACT(YEAR FROM v."Fecha") = EXTRACT(YEAR FROM CURRENT_DATE)-1) AS "MargenAñoAnterior"

				FROM vw_productos_descripcion vw
				INNER JOIN ventas v ON v."CodigoId" = vw."CodigoId" AND v."Fecha" BETWEEN $1 AND $2 AND v."Status" = 'V' ` 
				if(SucursalId != 0 ){
					sql+=`AND v."SucursalId" = $3 `
				}
				sql+=`INNER JOIN inventario_perpetuo ip ON ip."CodigoId" = vw."CodigoId" `
				
				if(SucursalId != 0){
					sql+=`AND ip."SucursalId" = $3 `  
				}else{
					sql+=`AND ip."SucursalId" = 3 `
				}
				
				sql +=`AND $3 = $3
				GROUP BY vw."CodigoId",vw."Descripcion",ip."PrecioVentaConImpuesto",ip."MargenReal"
				ORDER BY "UnidadesVendidas" DESC
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



/*
//Estas líneas se usan para https mediante el uso de certificados

//Crear el Servidor HTTPS
const httpsServer = https.createServer(options_https,app);
httpsServer.listen(8443, ()=>{
	console.log(`Server is running....... on Port ${port} PRODUCTION`)
});
*/
