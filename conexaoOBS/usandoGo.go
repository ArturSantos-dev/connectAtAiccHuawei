package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Estrutura para analisar a resposta XML
type ListBucketResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

func main() {
	// Suas credenciais do OBS
	accessKey := "your-access-key-id"
	secretKey := "your-secret-access-key"
	region := "your-region" // ex: sa-saopaulo-1 para São Paulo
	bucketName := "your-bucket-name"
	folderPrefix := "your-folder/" // Caminho da pasta no bucket

	// URL para listar os arquivos na pasta
	url := fmt.Sprintf("https://%s.obs.%s.myhuaweicloud.com/?prefix=%s", bucketName, region, folderPrefix)

	// Gerar a data no formato GMT
	date := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")

	// Gerar a autorização HMAC-SHA1
	authHeader := generateAuthorization(accessKey, secretKey, bucketName, "", date)

	// Criar a requisição HTTP
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Erro ao criar a requisição:", err)
		return
	}

	// Adicionar os cabeçalhos necessários
	req.Header.Set("Host", fmt.Sprintf("%s.obs.%s.myhuaweicloud.com", bucketName, region))
	req.Header.Set("Date", date)
	req.Header.Set("Authorization", authHeader)

	// Fazer a requisição
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Erro ao fazer a requisição:", err)
		return
	}
	defer resp.Body.Close()

	// Verificar o código de resposta
	if resp.StatusCode != 200 {
		fmt.Printf("Erro ao listar arquivos: %d %s\n", resp.StatusCode, resp.Status)
		return
	}

	// Analisar a resposta XML
	var result ListBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Println("Erro ao analisar a resposta XML:", err)
		return
	}

	// Baixar cada arquivo listado
	for _, content := range result.Contents {
		fileKey := content.Key
		fmt.Printf("Baixando arquivo: %s\n", fileKey)
		downloadFile(accessKey, secretKey, region, bucketName, fileKey)
	}
}

// Função para gerar a autorização HMAC-SHA1 e codificar em Base64
func generateAuthorization(accessKey, secretKey, bucketName, objectKey, date string) string {
	stringToSign := fmt.Sprintf("GET\n\n\n%s\n/%s/%s", date, bucketName, objectKey)
	h := hmac.New(sha1.New, []byte(secretKey))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("OBS %s:%s", accessKey, signature)
}

// Função para baixar o arquivo
func downloadFile(accessKey, secretKey, region, bucketName, fileKey string) {
	// URL para download do arquivo
	fileURL := fmt.Sprintf("https://%s.obs.%s.myhuaweicloud.com/%s", bucketName, region, fileKey)

	// Gerar a data no formato GMT
	date := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")

	// Gerar a autorização HMAC-SHA1
	authHeader := generateAuthorization(accessKey, secretKey, bucketName, fileKey, date)

	// Criar a requisição HTTP para download
	req, err := http.NewRequest("GET", fileURL, nil)
	if err != nil {
		fmt.Println("Erro ao criar a requisição para download:", err)
		return
	}

	// Adicionar os cabeçalhos necessários
	req.Header.Set("Host", fmt.Sprintf("%s.obs.%s.myhuaweicloud.com", bucketName, region))
	req.Header.Set("Date", date)
	req.Header.Set("Authorization", authHeader)

	// Fazer a requisição
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Erro ao baixar o arquivo:", err)
		return
	}
	defer resp.Body.Close()

	// Verificar o código de resposta
	if resp.StatusCode != 200 {
		fmt.Printf("Erro ao baixar arquivo %s: %d %s\n", fileKey, resp.StatusCode, resp.Status)
		return
	}

	// Criar um arquivo local com o nome correto
	localFilename := fileKey // O nome do arquivo será o mesmo que no OBS
	outFile, err := os.Create(localFilename)
	if err != nil {
		fmt.Println("Erro ao criar o arquivo:", err)
		return
	}
	defer outFile.Close()

	// Copiar o conteúdo da resposta HTTP para o arquivo local
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		fmt.Println("Erro ao salvar o arquivo:", err)
		return
	}

	fmt.Printf("Download concluído: %s\n", localFilename)
}
