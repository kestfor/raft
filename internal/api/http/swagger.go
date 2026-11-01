package httpapi

const swaggerPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Raft KV API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.33.3/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.33.3/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({url: '/openapi.yaml', dom_id: '#swagger-ui',
      deepLinking: true, validatorUrl: null, displayRequestDuration: true});
  </script>
</body>
</html>`
