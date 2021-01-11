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

app.get('/ingresos/sucursales/:naturalezaCC',authenticationToken,async(req, res) => {
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
		sql = `SELECT "CategoriaId","Categoria" FROM categorias`
	}

	if (id === '2'){
		sql = `SELECT "CategoriaId","SubcategoriaId","Subcategoria" FROM subcategorias`
	}
	if (id === '3'){
		sql = `SELECT "MedidaCapacidadId","MedidaCapacidad" FROM medidas_capacidad`
	}
	if (id === '4'){
		sql = `SELECT "MedidaVentaId","MedidaVenta" FROM medidas_venta`
	}
	if (id === '5'){
		sql = `SELECT "MarcaId","Marca" FROM marcas`
	}
	if ( id === '6'){
		sql = `SELECT "ColorId","Color" FROM colores`
	}
	if (id === '7'){
		sql = `SELECT "SaborId","Sabor" FROM sabores`
	}
	if (id === '8'){
		sql = `SELECT "IVAId","Descripcion", "IVA" FROM impuestos_iva`
	}
	if (id === '9'){
		sql = `SELECT "IEPSId","Descripcion", "IEPS" FROM impuestos_ieps`
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
	SaborId, IVAId, IVA, IEPSId, IEPS, Usuario } = req.body

	const client = await pool.connect()
	try{
		await client.query('BEGIN')
		let sql = `SELECT COALESCE(MAX("CodigoId"),0)+1 AS "CodigoId" FROM productos`
		let response = await client.query(sql) 
		const CodigoId = response.rows[0].CodigoId

		let values=[CodigoId,CodigoBarras, Descripcion, CategoriaId, SubcategoriaId, UnidadesCapacidad, MedidaCapacidadId, UnidadesVenta, MedidaVentaId, 
	    		    MarcaId, ColorId, SaborId, IVAId, IEPSId, Usuario]

		//INSERT productos
	     	sql = `INSERT INTO productos("CodigoId","CodigoBarras","Descripcion","CategoriaId","SubcategoriaId","UnidadesCapacidad","MedidaCapacidadId",
 	        	   "UnidadesVenta","MedidaVentaId","MarcaId","ColorId","SaborId","IVAId","IEPSId","Usuario","FechaHora") 
 			   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now()) RETURNING "CodigoId";
		`
		response = await client.query(sql,values)

		//INSERT codigos_barras
		values = [CodigoBarras, CodigoId, Usuario]
		sql = `INSERT INTO codigos_barras ("CodigoBarras","CodigoId","FechaHora","Usuario") VALUES ($1,$2,NOW(),$3)`
		await client.query(sql,values)

		//INSERT inventario_perpetuo
		sql = `SELECT "SucursalId" FROM sucursales WHERE "TipoSucursal" = 'S'`
		const arregloSucursales = await client.query(sql)

		//arregloSucursales.rows.forEach(async (element)=>{
		for (let i=0; i < arregloSucursales.rows.length; i++){
			values=[arregloSucursales.rows[i].SucursalId,CodigoId,0,0.00,0.00,30,0.00,0.00,IVAId,IVA,0.00,IEPSId,IEPS,0.00,0.00,6,2,"now()",null,null,null,0.00]

			sql = `INSERT INTO inventario_perpetuo ("SucursalId","CodigoId","UnidadesExistencia","CostoCompra","CostoPromedio","Margen","MargenReal",
			"PrecioVentaSinImpuesto","IVAId","IVA","IVAMonto","IEPSId","IEPS","IEPSMonto","PrecioVentaConImpuesto","Maximo","Minimo","FechaHora","FechaCambioPrecio",
			"FechaUltimaVenta","FechaUltimaCompra","CostoBasePrecioVenta")
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
			`
			await client.query(sql, values)
		}

		await client.query('COMMIT')
		res.status(200).json({"Success":response.rows[0].CodigoId})

	}catch(error){
		console.log(error.message)
		await client.query('ROLLBACK')
		res.status(500).json({"error":error.message})
	}finally{
		client.release()
	}
})

app.get('/api/consultaProductosRecientes',authenticationToken, async(req, res)=>{

	//let sql = `SELECT "CodigoId","CodigoBarras","Descripcion" FROM productos ORDER BY "CodigoId" DESC LIMIT 10`
	let sql = `SELECT "CodigoId","CodigoBarras","Descripcion" FROM vw_productos_descripcion ORDER BY "CodigoId" DESC LIMIT 10`
	let response;

	try{
		const response = await pool.query(sql)
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

const port = process.env.PORT || 3001

app.listen(port, ()=>{console.log(`Server is running.... on Port ${port}`)})
