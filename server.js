const express = require('express')
const app = express()

//routes
app.get('/', (req,res)=>{
	res.send('Hello AWS Testing Server')
})

PORT = process.env.PORT || 3001

app.listen(PORT, ()=>{console.log(`Server is running.... on Port ${PORT}`)})
