# Usar una imagen oficial de Node.js como base
FROM node:18-alpine

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar archivos de dependencias e instalarlas
COPY package*.json ./
RUN npm install

# Copiar el resto de los archivos de la aplicación
COPY . .