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
	let sql = `SELECT "SucursalId","Sucursal" AS "Sucursal" FROM sucursales 
		   WHERE "Status" = $1 `
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
	const vsucursal = req.body.SucursalId

	let values = []
	const client = await pool.connect();
	let sql = ''

	try{
		await client.query('BEGIN')
		values = [vsucursal,"now()"]
		sql = `INSERT INTO tablaprueba ("SucursalId") VALUES ($1,$2) RETURNING "SucursalId"
		`
		await client.query(sql,values)
		await client.query('COMMIT')
		res.status(200).send("Success!!!")
	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(400).send(error.message)
	}finally{
		client.release()
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
