package api

// @title Nebula Conduit API
// @version 1.0
// @description API for semantic search and embeddings management
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.email support@nebulaapi.com

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
// @schemes http https

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Enter your bearer token in the format **Bearer &lt;token&gt;**

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"Nebula.Conduit/services"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
)

type WebAPI struct {
	router           *mux.Router
	embeddingService services.EmbeddingService
	jwtSecret        []byte
}

type SearchRequest struct {
	Collection string `json:"collection"`
	Query      string `json:"query"`
}

type Claims struct {
	Username string `json:"username"`
	jwt.StandardClaims
}

func NewWebAPI(embeddingService services.EmbeddingService) *WebAPI {
	api := &WebAPI{
		router:           mux.NewRouter(),
		embeddingService: embeddingService,
		jwtSecret:        []byte("your-secret-key"), // Change this in production
	}
	api.setupRoutes()
	return api
}

// healthCheck handles health check requests
func (api *WebAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (api *WebAPI) setupRoutes() {
	api.router.HandleFunc("/login", api.login).Methods("POST")
	api.router.HandleFunc("/search", api.authenticateMiddleware(api.search)).Methods("POST")
	api.router.HandleFunc("/", api.healthCheck).Methods("GET")
}

func (api *WebAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.router.ServeHTTP(w, r)
}

func (api *WebAPI) authenticateMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		bearerToken := strings.Split(authHeader, " ")
		if len(bearerToken) != 2 {
			http.Error(w, "Invalid token format", http.StatusUnauthorized)
			return
		}

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(bearerToken[1], claims, func(token *jwt.Token) (interface{}, error) {
			return api.jwtSecret, nil
		})

		if err != nil || !token.Valid {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func (api *WebAPI) login(w http.ResponseWriter, r *http.Request) {
	// In a real application, validate credentials from request body
	// This is a simplified example
	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		Username: "user", // Replace with actual username
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(api.jwtSecret)
	if err != nil {
		http.Error(w, "Error creating token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token": tokenString,
	})
}

func (api *WebAPI) search(w http.ResponseWriter, r *http.Request) {
	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Collection == "" || req.Query == "" {
		http.Error(w, "Collection and query are required", http.StatusBadRequest)
		return
	}

	rawResults := api.embeddingService.SearchResults(req.Collection, req.Query)
	results := make([]map[string]interface{}, 0)
	for _, rawResult := range rawResults {
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(rawResult), &result); err != nil {
			log.Printf("Error unmarshaling result: %v", err)
			continue
		}
		results = append(results, result)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}
