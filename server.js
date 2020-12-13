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
	let sql = `SELECT "Password" FROM colaboradores WHERE "User" = $1 UNION ALL SELECT '0'`
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
		res.status(200).json({ "error":'', "user": user, "accessToken": accessToken})
	}else{
		//res.status(401).send("Password Incorrecto") 
		res.status(401).json({"error":"Password Incorrecto"}) 
	}

})

app.get('/ingresos/sucursales',authenticationToken,async(req, res) => {
	let sql = `SELECT "SucursalId","Sucursal" AS "Sucursal","SucursalNombre","TipoSucursal" FROM sucursales 
		   WHERE "Status" = $1
		   ORDER BY "SucursalId"
	`
	let response
	const values = ['A']
	try{
		response = await pool.query(sql,values) 
		let data = response.rows
		res.status(200).json(data)
	}catch(error){
		console.log(error.message)
		res.status(500).json({"error": error.message})
	}
})


app.get('/ingresos/unidadesdenegociocatalogo',authenticationToken,async(req, res) => {
	let sql = `SELECT DISTINCT cc."SucursalId", udn."UnidadDeNegocioId", udn."UnidadDeNegocio" 
			FROM unidades_de_negocio udn
			INNER JOIN catalogo_contable cc ON cc."UnidadDeNegocioId" = udn."UnidadDeNegocioId"
			ORDER BY udn."UnidadDeNegocioId"
	`
	let values = []
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

app.get('/ingresos/cuentascontablescatalogo',authenticationToken, async(req, res) => {
	let sql = `SELECT DISTINCT cac."SucursalId",cac."UnidadDeNegocioId",cc."CuentaContableId",cc."CuentaContable"
			FROM cuentas_contables cc
        		INNER JOIN catalogo_contable cac ON cac."CuentaContableId" = cc."CuentaContableId"
			ORDER BY cc."CuentaContableId"
	`
	let response;
	const values = []
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

app.get('/ingresos/subcuentascontablescatalogo',authenticationToken, async(req,res) => {
	let sql = `SELECT DISTINCT cc."SucursalId",cc."UnidadDeNegocioId",cc."CuentaContableId", cc."SubcuentaContableId", scc."SubcuentaContable"
			FROM subcuentas_contables scc
			INNER JOIN catalogo_contable cc ON cc."CuentaContableId" = scc."CuentaContableId" AND cc."SubcuentaContableId" = scc."SubcuentaContableId"
	`
	let response
	const values = []
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


	let values = []
	const client = await pool.connect();
	let sql = ''

	try{
		await client.query('BEGIN')
		values = [vsucursalid,vunidaddenegocioid,vcuentacontableid,vsubcuentacontableid,vcomentarios,vfecha,'202001',vmonto,'P',"now()",'pendiente','now()']
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


app.get('/ingresos/getIngresos/:fecha',authenticationToken,async(req, res)=> {
	const vfecha = req.params.fecha


	let sql = `SELECT rc."SucursalId",rc."FolioId",s."SucursalNombre",udn."UnidadDeNegocioNombre",cc."CuentaContable",
		scc."SubcuentaContable",rc."Fecha",rc."Monto"
		FROM registro_contable rc
		INNER JOIN sucursales s ON rc."SucursalId"=s."SucursalId"
		INNER JOIN unidades_de_negocio udn ON rc."UnidadDeNegocioId"=udn."UnidadDeNegocioId"
		INNER JOIN cuentas_contables cc ON rc."CuentaContableId" = cc."CuentaContableId"
		INNER JOIN subcuentas_contables scc ON rc."CuentaContableId"= scc."CuentaContableId" AND rc."SubcuentaContableId" = scc."SubcuentaContableId"
		WHERE rc."Fecha" = $1
		`
	
	let response;
	//const values=[vsucursal,vfecha]
	const values=[vfecha]
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
	let sql = `SELECT "Periodo" FROM cierres_mes
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

PORT = process.env.PORT || 3001

app.listen(PORT, ()=>{console.log(`Server is running.... on Port ${PORT}`)})
