package main

import (
	"context"
	"encoding/json"
	"github.com/dgrijalva/jwt-go"
	"github.com/untillpro/airs-iconfig"
	"golang.org/x/crypto/bcrypt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const signedString = "hello"

type Token struct {
	UserId uint
	jwt.StandardClaims
}

type Account struct {
	Login    string `json:"login"`
	Password string `json:"password"`
	Token    string `json:"token"`
}

type Resp struct {
	Token string `json:"token"`
	Exp   int64  `json:"exp"`
}

func (account *Account) Validate(ctx context.Context) (string, bool) {

	if len(account.Password) == 0 {
		return "Password is required", false
	}

	if len(account.Password) < 6 {
		return "Password should be longer than 6 symbols", false
	}

	var temp Account
	err := iconfig.GetConfig(ctx, account.Login, &temp)
	if err == nil {
		return "Login address already in use", false
	}

	return "Requirement passed", true
}

func (account *Account) Create(ctx context.Context) []byte {
	rand.Seed(time.Now().UnixNano())

	if resp, ok := account.Validate(ctx); !ok {
		return []byte(resp)
	}

	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(account.Password), bcrypt.DefaultCost)
	account.Password = string(hashedPassword)

	err := iconfig.PutConfig(ctx, account.Login, &account)
	if err != nil {
		return []byte("Can't put account to KV")
	}

	resp := create72HourToken()

	buf, err := json.Marshal(resp)
	if err != nil {
		return []byte("Can't marshal account")
	}

	return buf
}

func create72HourToken() *Resp {
	tk := &Token{UserId: uint(rand.Intn(100000))}
	tk.ExpiresAt = time.Now().Add(time.Hour * 72).Unix()
	token := jwt.NewWithClaims(jwt.GetSigningMethod("HS256"), tk)
	tokenString, _ := token.SignedString([]byte(os.Getenv(signedString)))
	return &Resp{
		Token: tokenString,
		Exp:   tk.ExpiresAt,
	}
}

func Login(ctx context.Context, login, password string) []byte {

	var account Account
	err := iconfig.GetConfig(ctx, login, &account)
	if err != nil {
		return []byte("Login address not found")
	}

	err = bcrypt.CompareHashAndPassword([]byte(account.Password), []byte(password))
	if err != nil && err == bcrypt.ErrMismatchedHashAndPassword { //Password does not match!
		return []byte("Invalid login credentials. Please try again")
	}

	//Create JWT token
	resp := create72HourToken()

	buf, err := json.Marshal(resp)
	if err != nil {
		return []byte("Can't marshal account")
	}

	return buf
}

var JwtAuthentication = func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notAuth := []string{"/user/new", "/user/login"}
		requestPath := r.URL.Path
		for _, value := range notAuth {
			if value == requestPath {
				next.ServeHTTP(w, r)
				return
			}
		}
		tokenHeader := r.Header.Get("Authorization")
		if tokenHeader == "" {
			http.Error(w, "missing auth token", http.StatusUnauthorized)
			return
		}

		splitted := strings.Split(tokenHeader, " ") //The token normally comes in format `Bearer {token-body}`, we check if the retrieved token matched this requirement
		if len(splitted) != 2 {
			http.Error(w, "Invalid/Malformed auth token", http.StatusUnauthorized)
			return
		}

		tokenPart := splitted[1] //Grab the token part, what we are truly interested in

		var tk Token

		token, err := jwt.ParseWithClaims(tokenPart, &tk, func(token *jwt.Token) (interface{}, error) {
			return []byte(os.Getenv(signedString)), nil
		})

		if err != nil { //Malformed token, returns with http code 403 as usua
			http.Error(w, "Malformed authentication token", http.StatusUnauthorized)
			return
		}

		if !token.Valid { //Token is invalid, maybe not signed on this server
			http.Error(w, "Token is not valid", http.StatusUnauthorized)
			return
		}

		//Everything went well, proceed with the request and set the caller to the user retrieved from the parsed token
		log.Println("User", tk.UserId) //Useful for monitoring
		ctx := context.WithValue(r.Context(), "user", tk.UserId)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r) //proceed in the middleware chain!
	})
}

func (s *Service) CreateAccount(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		var acc Account
		err := json.NewDecoder(req.Body).Decode(&acc)
		if err != nil {
			http.Error(resp, "invalid request", http.StatusBadRequest)
			return
		}
		newAcc := acc.Create(ctx)
		resp.Write(newAcc)
	}
}

func (s *Service) Authenticate(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		var acc Account
		err := json.NewDecoder(req.Body).Decode(&acc)
		if err != nil {
			http.Error(resp, "invalid request", http.StatusBadRequest)
			return
		}
		newAcc := Login(ctx, acc.Login, acc.Password)
		resp.Write(newAcc)
	}
}
