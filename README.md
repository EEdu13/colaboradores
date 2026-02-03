# Sistema de Upload de Colaboradores RH

Sistema web para importar planilha Excel de colaboradores e sincronizar automaticamente com Azure SQL Server.

## 🚀 Como Usar

### 1. Instalar dependências
```bash
npm install
```

### 2. Executar
```bash
npm start
```

O servidor vai iniciar em `http://localhost:3000`

### 3. Usar o sistema
1. Abra o navegador em `http://localhost:3000`
2. Faça upload da planilha `TODOS.xlsx`
3. Revise o preview dos dados
4. Clique em "Sincronizar com SQL"
5. Pronto! Os dados foram atualizados no Azure SQL

## 📋 O que o sistema faz

1. **Processa a planilha** exatamente como o Power Query:
   - Detecta automaticamente as empresas
   - Extrai dados das colunas corretas (sem depender de nomes de header)
   - Limpa e valida CPF
   - Normaliza cargos
   - Calcula matrícula com sufixo da empresa

2. **Mapeia automaticamente**:
   - CNPJ da empresa
   - Projeto baseado no centro de custo
   - Equipe
   - Classe do cargo
   - Nome do líder (se for líder/coordenador)

3. **Sincroniza com SQL**:
   - DELETE FROM COLABORADORES (limpa tudo)
   - INSERT de todos os registros ativos
   - Transação completa (rollback em caso de erro)

## 🏢 Empresas Mapeadas

- DS3 FLORESTAL LTDA → CNPJ: 46.002.274/0001-10 → Sufixo: 4
- LARSIL FLORESTAL LTDA → CNPJ: 08.420.245/0001-80 → Sufixo: 1
- S5 FLORESTAL MATRIZ → CNPJ: 53.289.524/0001-00 → Sufixo: 3
- ALR FLORESTAL EMPREENDIMENTOS LTDA → CNPJ: 52.387.856/0001-65 → Sufixo: 2

## 📊 Tabela Azure SQL

```sql
COLABORADORES (
    ID INT IDENTITY PRIMARY KEY,
    NOME NVARCHAR(100),
    FUNCAO NVARCHAR(100),
    CPF VARCHAR(11),
    DATA_ADMISSAO DATE,
    PROJETO VARCHAR(10),
    EQUIPE VARCHAR(20),
    COORDENADOR NVARCHAR(100),
    SUPERVISOR NVARCHAR(100),
    HORAS_TRABALHADAS INT,
    FUNCAO_EXECUTANTE NVARCHAR(100),
    CLASSE VARCHAR(10),
    ATUALIZADO_EM DATE,
    NOME_LIDER VARCHAR(255),
    CNPJ VARCHAR(18),
    EMPRESA NVARCHAR(255),
    MATRICULA VARCHAR(20)
)
```

## 🔧 Deploy no Railway

1. Crie novo projeto no Railway
2. Conecte o repositório
3. Configure a porta: `PORT=3000`
4. Deploy automático!

## 📝 Estrutura do Projeto

```
/
├── server.js          # Backend completo
├── public/
│   └── index.html     # Frontend único
├── uploads/           # Arquivos temporários (criado automaticamente)
├── package.json
├── .env.example
└── README.md
```

## ⚠️ Observações

- Apenas colaboradores com SITUACAO = 1 (ativos) são processados
- CPF deve ter 11 dígitos
- Data de admissão é obrigatória
- A sincronização substitui TODOS os dados (DELETE + INSERT)

## 🆘 Problemas Comuns

**Erro de conexão SQL:**
- Verifique se o firewall do Azure permite seu IP
- Teste conexão: http://localhost:3000/api/health

**Planilha não processa:**
- Certifique-se que é arquivo .xlsx
- Verifique se tem dados de colaboradores (linhas com código numérico)

**Registros não aparecem:**
- Apenas colaboradores ativos (SITUACAO = 1) são importados
- CPF deve ser válido (11 dígitos)