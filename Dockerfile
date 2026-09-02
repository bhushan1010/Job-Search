# Apify actor Dockerfile
FROM apify/actor-node:20

# Copy package definitions
COPY package*.json ./

# Install production dependencies
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed dependencies"

# Copy source code
COPY . ./

# Run the actor
CMD ["npm", "start"]
