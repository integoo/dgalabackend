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

PORT = process.env.PORT || 3001

app.listen(PORT, ()=>{console.log(`Server is running.... on Port ${PORT}`)})
