-- =============================================================================
-- SCRIPT: Criar tabela COLABORADORES_HISTORICO
-- Descrição: Tabela para armazenar histórico de colaboradores
-- Mesmas colunas de COLABORADORES + DATA do registro
-- =============================================================================

-- Criar tabela de histórico
CREATE TABLE COLABORADORES_HISTORICO (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Colunas iguais à COLABORADORES
    NOME NVARCHAR(255),
    FUNCAO NVARCHAR(255),
    CPF VARCHAR(11),
    DATA_ADMISSAO DATE,
    PROJETO VARCHAR(10),
    PROJETO_RH NVARCHAR(100),
    SITUACAO NVARCHAR(50),
    SITUACAO_TIPO NVARCHAR(100),
    EQUIPE VARCHAR(20),
    COORDENADOR NVARCHAR(100),
    SUPERVISOR NVARCHAR(100),
    HORAS_TRABALHADAS INT DEFAULT 8,
    FUNCAO_EXECUTANTE NVARCHAR(100),
    CLASSE VARCHAR(10),
    NOME_LIDER VARCHAR(255),
    CNPJ VARCHAR(18),
    EMPRESA NVARCHAR(255),
    MATRICULA VARCHAR(20),
    ATUALIZADO_EM DATE,
    
    -- Coluna adicional para histórico
    DATA_REGISTRO DATETIME DEFAULT GETDATE()
);

-- Criar índices para melhor performance nas consultas
CREATE INDEX IX_HISTORICO_CPF ON COLABORADORES_HISTORICO(CPF);
CREATE INDEX IX_HISTORICO_DATA_REGISTRO ON COLABORADORES_HISTORICO(DATA_REGISTRO);
CREATE INDEX IX_HISTORICO_EMPRESA ON COLABORADORES_HISTORICO(EMPRESA);
CREATE INDEX IX_HISTORICO_SITUACAO ON COLABORADORES_HISTORICO(SITUACAO);

-- Comentário na tabela
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Tabela de histórico de colaboradores - registra snapshots dos dados ao longo do tempo',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'COLABORADORES_HISTORICO';

PRINT '✅ Tabela COLABORADORES_HISTORICO criada com sucesso!';
GO
