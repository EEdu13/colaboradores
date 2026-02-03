require('dotenv').config();
const express = require('express');
const multer = require('multer');
const xlsx = require('xlsx');
const sql = require('mssql');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const nodemailer = require('nodemailer');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

// =============================================================================
// CONFIGURAÇÕES
// =============================================================================

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

// Configuração do Email - Microsoft 365 / Outlook
// EMAIL_ENABLED=false para desabilitar envio de email
const EMAIL_ENABLED = process.env.EMAIL_ENABLED !== 'false';

// Configuração SMTP - Microsoft 365
const emailTransporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST || 'smtp.office365.com',
    port: parseInt(process.env.EMAIL_PORT) || 587,
    secure: false, // Office 365 usa STARTTLS
    requireTLS: true,
    connectionTimeout: 30000,
    greetingTimeout: 30000,
    socketTimeout: 30000,
    auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS
    },
    tls: {
        ciphers: 'SSLv3',
        rejectUnauthorized: false
    }
});

// Função para enviar email sem bloquear (fire and forget)
function enviarEmailAsync(mailOptions) {
    if (!EMAIL_ENABLED) {
        console.log('📧 Email desabilitado (EMAIL_ENABLED=false)');
        return;
    }
    
    if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
        console.log('📧 Email não configurado (EMAIL_USER/EMAIL_PASS não definidos)');
        return;
    }
    
    // Enviar sem await - não bloqueia a resposta
    emailTransporter.sendMail(mailOptions)
        .then(() => console.log(`📧 Email enviado para: ${mailOptions.to}`))
        .catch(err => console.error('❌ Erro ao enviar email:', err.message));
}

// Lista de emails para receber o relatório (pode adicionar mais)
const EMAIL_DESTINATARIOS = [
    'ferreira.eduardo@larsil.com.br'
];

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = './uploads';
        if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        cb(null, uniqueSuffix + '-' + file.originalname);
    }
});

const upload = multer({
    storage: storage,
    fileFilter: (req, file, cb) => {
        const ext = path.extname(file.originalname).toLowerCase();
        if (ext === '.xlsx') cb(null, true);
        else cb(new Error('Apenas arquivos .xlsx são permitidos. Salve o Excel como "Pasta de Trabalho do Excel (.xlsx)"'), false);
    },
    limits: { fileSize: 10 * 1024 * 1024 }
});

// Cache temporário dos dados processados (evita reenviar 2700 registros)
let cachedUploadData = null;
let cacheTimestamp = null;
const CACHE_DURATION = 30 * 60 * 1000; // 30 minutos

// Configuração Azure SQL (credenciais em variáveis de ambiente)
const sqlConfig = {
    user: process.env.SQL_USER || 'sqladmin',
    password: process.env.SQL_PASSWORD,
    database: process.env.SQL_DATABASE || 'Tabela_teste',
    server: process.env.SQL_SERVER || 'alrflorestal.database.windows.net',
    port: parseInt(process.env.SQL_PORT) || 1433,
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    options: {
        encrypt: true,
        trustServerCertificate: false
    }
};

// =============================================================================
// MAPEAMENTOS (baseado na tabela do cliente)
// =============================================================================

const EMPRESA_CNPJ = {
    'DS3 FLORESTAL LTDA': '46.002.274/0001-10',
    'DS3 FLORESTAL MATRIZ': '46.002.274/0001-10',
    'LARSIL FLORESTAL LTDA': '08.420.245/0001-80',
    'LARSIL FLORESTAL MATRIZ': '08.420.245/0001-80',
    'S5 FLORESTAL MATRIZ': '53.289.524/0001-00',
    'S5 FLORESTAL LTDA': '53.289.524/0001-00',
    'ALR FLORESTAL EMPREENDIMENTOS LTDA': '52.387.856/0001-65',
    'ALR FLORESTAL MATRIZ MS': '52.387.856/0001-65',
    'ALR FLORESTAL EMPREENDIMENTOS': '52.387.856/0001-65'
};

const EMPRESA_SUFIXO = {
    'DS3 FLORESTAL LTDA': '4',
    'DS3 FLORESTAL MATRIZ': '4',
    'LARSIL FLORESTAL LTDA': '1',
    'LARSIL FLORESTAL MATRIZ': '1',
    'S5 FLORESTAL MATRIZ': '3',
    'S5 FLORESTAL LTDA': '3',
    'ALR FLORESTAL EMPREENDIMENTOS LTDA': '2',
    'ALR FLORESTAL MATRIZ MS': '2',
    'ALR FLORESTAL EMPREENDIMENTOS': '2'
};

// MAPEAMENTO SITUAÇÃO → TIPO (nunca muda)
const SITUACAO_TIPO_MAP = {
    1: 'Trabalhando',
    2: 'Afastado Direitos Integrais',
    3: 'Acid. Trabalho periodo superior a 15 dias',
    4: 'Servico Militar',
    5: 'Licenca maternidade',
    6: 'Doenca periodo superior a 15 dias',
    7: 'Licenca sem Vencimento',
    8: 'Demitido',  // NÃO SOBE PARA SQL
    9: 'Ferias',
    10: 'Novo afast. mesmo acid. trabalho',
    11: 'Antecipacao e/ou prorrogacao Licenca Maternidade',
    12: 'Novo afast. mesma doenca',
    13: 'Exercicio de mandato sindical',
    14: 'Aposent. por invalid. acidente de trabalho',
    15: 'Aposent. por invalid. doenca profissional',
    16: 'Aposent. por invalid. exceto acid. trab. e doenca profissional',
    17: 'Acid. Trabalho periodo igual ou inferior a 15 dias',
    18: 'Doenca periodo igual ou inferior a 15 dias',
    19: 'Aborto nao criminoso',
    20: 'Licenca maternidade adocao 1 ano',
    21: 'Licenca maternidade adocao 1 a 4 anos',
    22: 'Licenca maternidade adocao 4 a 8 anos',
    24: 'Outros motivos de afastamento',
    90: 'Suspensao contratual decorrente acao trabalhista por rescisao indireta',
    91: 'Suspensao contratual para inquerito de apuracao de falta grave'
};

// Função para obter o tipo de situação
function obterSituacaoTipo(situacao) {
    const num = parseInt(situacao);
    return SITUACAO_TIPO_MAP[num] || '';
}

// MAPEAMENTO CARGO → CLASSE (da tabela do cliente)
const CARGO_CLASSE = {
    'OP. MAQ. FLORESTAIS I': 'OPF',
    'OPERADOR DE CAMINHAO MUNCK': 'OPF',
    'OP DE PÁ CARREGADEIRA': 'OPF',
    'OPERADOR DE PA CARREGADEIRA': 'OPF',
    'OPERADOR DE MUNCK': 'OPF',
    'COORDENADOR': 'COF',
    'COORDENADOR DE SIVILCUTURA': 'COF',
    'COORDENADOR SILVICULTURA': 'COF',
    'AUXILIAR DE LIDER': 'LDF',
    'LIDER DE EQUIPE': 'LDF',
    'LIDER DE MANUTENCAO': 'LDF',
    'LIDER DE MAQUINAS PESADAS': 'LDF',
    'LIDER FLORESTAL': 'LDF',
    'BORRACHEIRO DE CAMPO': 'MEC',
    'MECANICO': 'MEC',
    'MECANICO I': 'MEC',
    'AUX. MECANICO': 'MEC',
    'MOTORISTA': 'MCM',
    'MOTORISTA CAMINHAO': 'MCM',
    'MOTORISTA CAMINHAO PIPA': 'MCM',
    'MOTORISTA COMBOIO': 'MCM',
    'MOTORISTA CARRETA CACAMBA': 'MCR',
    'MOTORISTA CAMINHAO PRANCHA': 'MCM',
    'SUPERVISOR SILVICULTURA': 'SPF',
    'SUPERVISOR SILVICULTURA E FROTA': 'SPF',
    'TRABALHADOR FLORESTAL': 'TRF',
    'ANALISTA ADMINISTRATIVO': 'ADM',
    'ANALISTA ADMINISTRATIVO I': 'ADM',
    'ANALISTA DE PLANEJAMENTO': 'ADM',
    'ANALISTA DE PLANEJAMENTO JR': 'ADM',
    'ANALISTA SEGURANÇA DO TRABALHO': 'ADM',
    'ANALISTA DE SAUDE OCUPACIONAL': 'ADM',
    'APRENDIZ ADMINISTRATIVO': 'ADM',
    'ASSISTENTE ADMINISTRATIVO': 'ADM',
    'ASSISTENTE DE PCP': 'ADM',
    'ASSISTENTE DE PCP I': 'ADM',
    'ASSISTENTE DE TECNOLOGIA DA INFORMACAO': 'ADM',
    'AUXILIAR ADMINISTRATIVO': 'ADM',
    'AUXILIAR ADMINISTRATIVO I': 'ADM',
    'AUXILIAR DE CONTROLE': 'ADM',
    'AUXILIAR DE QUALIDADE': 'ADM',
    'AUXILIAR FINANCEIRO': 'ADM',
    'AUXILIAR TECNICO DE QUALIDADE': 'ADM',
    'COMPRADOR': 'ADM',
    'COMPRADOR JUNIOR': 'ADM',
    'ESTAGIÁRIO ADMINISTRATIVO': 'ADM',
    'GERENTE ADMINISTRATIVO': 'ADM',
    'GERENTE FINANCEIRO': 'ADM',
    'GERENTE OPERACIONAL': 'ADM',
    'MONITOR DE ALOJAMENTO': 'ADM',
    'MONITOR FLORESTAL': 'ADM',
    'SERVICOS GERAIS': 'ADM',
    'SUPERVISOR ADMINISTRATIVO': 'ADM',
    'SUPERVISOR DE SUPRIMENTOS': 'ADM',
    'SUPERVISOR(A) ADMINISTRATIVO': 'ADM',
    'SUPERVISOR(A) FINANCEIRO': 'ADM',
    'SUPERVISOR DE PCP': 'ADM',
    'TECNICO DE QUALIDADE': 'ADM',
    'TECNICO SEGURANCA DO TRABALHO': 'ADM',
    'TECNICO SEGURANCA DO TRABALHO PLENO': 'ADM',
    'TECNICO EM SEGURANCA DO TRABALHO': 'ADM',
    'ANALISTA DE PCP JUNIOR': 'ADM'
};

// MAPEAMENTO CARGO → FUNCAO_EXECUTANTE (da tabela do cliente)
const CARGO_FUNCAO_EXECUTANTE = {
    'OP. MAQ. FLORESTAIS I': 'OPERADOR',
    'OPERADOR DE CAMINHAO MUNCK': 'OPERADOR',
    'OP DE PÁ CARREGADEIRA': 'OPERADOR',
    'OPERADOR DE PA CARREGADEIRA': 'OPERADOR',
    'OPERADOR DE MUNCK': 'OPERADOR',
    'COORDENADOR': 'COORDENADOR',
    'COORDENADOR DE SIVILCUTURA': 'COORDENADOR',
    'COORDENADOR SILVICULTURA': 'COORDENADOR',
    'AUXILIAR DE LIDER': 'LIDER',
    'LIDER DE EQUIPE': 'LIDER',
    'LIDER DE MANUTENCAO': 'LIDER',
    'LIDER DE MAQUINAS PESADAS': 'LIDER',
    'LIDER FLORESTAL': 'LIDER',
    'BORRACHEIRO DE CAMPO': 'MECANICO',
    'MECANICO': 'MECANICO',
    'MECANICO I': 'MECANICO',
    'AUX. MECANICO': 'MECANICO',
    'MOTORISTA': 'MOTORISTA',
    'MOTORISTA CAMINHAO': 'MOTORISTA',
    'MOTORISTA CAMINHAO PIPA': 'MOTORISTA',
    'MOTORISTA COMBOIO': 'MOTORISTA',
    'MOTORISTA CARRETA CACAMBA': 'MOTORISTA',
    'MOTORISTA CAMINHAO PRANCHA': 'MOTORISTA',
    'SUPERVISOR SILVICULTURA': 'SUPERVISOR',
    'SUPERVISOR SILVICULTURA E FROTA': 'SUPERVISOR',
    'TRABALHADOR FLORESTAL': 'TRABALHADOR',
    'ANALISTA ADMINISTRATIVO': 'ADMINISTRATIVO',
    'ANALISTA ADMINISTRATIVO I': 'ADMINISTRATIVO',
    'ANALISTA DE PLANEJAMENTO': 'ADMINISTRATIVO',
    'ANALISTA DE PLANEJAMENTO JR': 'ADMINISTRATIVO',
    'ANALISTA SEGURANÇA DO TRABALHO': 'ADMINISTRATIVO',
    'ANALISTA DE SAUDE OCUPACIONAL': 'ADMINISTRATIVO',
    'APRENDIZ ADMINISTRATIVO': 'ADMINISTRATIVO',
    'ASSISTENTE ADMINISTRATIVO': 'ADMINISTRATIVO',
    'ASSISTENTE DE PCP': 'ADMINISTRATIVO',
    'ASSISTENTE DE PCP I': 'ADMINISTRATIVO',
    'ASSISTENTE DE TECNOLOGIA DA INFORMACAO': 'ADMINISTRATIVO',
    'AUXILIAR ADMINISTRATIVO': 'ADMINISTRATIVO',
    'AUXILIAR ADMINISTRATIVO I': 'ADMINISTRATIVO',
    'AUXILIAR DE CONTROLE': 'ADMINISTRATIVO',
    'AUXILIAR DE QUALIDADE': 'ADMINISTRATIVO',
    'AUXILIAR FINANCEIRO': 'ADMINISTRATIVO',
    'AUXILIAR TECNICO DE QUALIDADE': 'ADMINISTRATIVO',
    'COMPRADOR': 'ADMINISTRATIVO',
    'COMPRADOR JUNIOR': 'ADMINISTRATIVO',
    'ESTAGIÁRIO ADMINISTRATIVO': 'ADMINISTRATIVO',
    'GERENTE ADMINISTRATIVO': 'ADMINISTRATIVO',
    'GERENTE FINANCEIRO': 'ADMINISTRATIVO',
    'GERENTE OPERACIONAL': 'ADMINISTRATIVO',
    'MONITOR DE ALOJAMENTO': 'ADMINISTRATIVO',
    'MONITOR FLORESTAL': 'ADMINISTRATIVO',
    'SERVICOS GERAIS': 'ADMINISTRATIVO',
    'SUPERVISOR ADMINISTRATIVO': 'ADMINISTRATIVO',
    'SUPERVISOR DE SUPRIMENTOS': 'ADMINISTRATIVO',
    'SUPERVISOR(A) ADMINISTRATIVO': 'ADMINISTRATIVO',
    'SUPERVISOR(A) FINANCEIRO': 'ADMINISTRATIVO',
    'SUPERVISOR DE PCP': 'ADMINISTRATIVO',
    'TECNICO DE QUALIDADE': 'ADMINISTRATIVO',
    'TECNICO SEGURANCA DO TRABALHO': 'ADMINISTRATIVO',
    'TECNICO SEGURANCA DO TRABALHO PLENO': 'ADMINISTRATIVO',
    'TECNICO EM SEGURANCA DO TRABALHO': 'ADMINISTRATIVO',
    'ANALISTA DE PCP JUNIOR': 'ADMINISTRATIVO'
};

// =============================================================================
// FUNÇÕES DE PROCESSAMENTO (igual Power Query)
// =============================================================================

function limparCPF(cpf) {
    if (!cpf) return null;
    const limpo = String(cpf).replace(/\D/g, '');
    return limpo.padStart(11, '0');
}

function normalizarCargo(cargo) {
    if (!cargo) return null;
    let normalizado = String(cargo).toUpperCase().trim();
    normalizado = normalizado.replace(/\s+/g, ' ');
    return normalizado;
}

function obterClasse(cargo) {
    const cargoNorm = normalizarCargo(cargo);
    return CARGO_CLASSE[cargoNorm] || 'OUT';
}

function obterFuncaoExecutante(cargo) {
    const cargoNorm = normalizarCargo(cargo);
    // Primeiro tenta o mapeamento exato
    if (CARGO_FUNCAO_EXECUTANTE[cargoNorm]) {
        return CARGO_FUNCAO_EXECUTANTE[cargoNorm];
    }
    // Se não encontrar, usa primeira palavra
    if (cargo) {
        const palavras = cargo.split(' ');
        return palavras[0].toUpperCase();
    }
    return null;
}

function gerarMatricula(codigo, empresa) {
    const sufixo = EMPRESA_SUFIXO[empresa] || '0';
    const codigoStr = String(codigo).padStart(4, '0');
    return sufixo + codigoStr;
}

/**
 * PROCESSA A PLANILHA IGUAL POWER QUERY
 * PROJETO, EQUIPE, COORDENADOR, SUPERVISOR, NOME_LIDER serão mantidos do SQL
 */
function processarPlanilhaTODOS(worksheet) {
    const range = xlsx.utils.decode_range(worksheet['!ref']);
    const dados = [];
    let empresaAtual = null;

    for (let rowNum = range.s.r; rowNum <= range.e.r; rowNum++) {
        // Pegar valor da coluna 0 (A)
        const cellA = worksheet[xlsx.utils.encode_cell({ r: rowNum, c: 0 })];
        const valorA = cellA ? cellA.v : null;

        if (!valorA) continue;

        // Detectar linha de EMPRESA (texto longo com "LTDA")
        if (typeof valorA === 'string' && valorA.length > 10 && valorA.toUpperCase().includes('LTDA')) {
            empresaAtual = valorA.trim().toUpperCase();
            console.log(`🏢 Empresa detectada: ${empresaAtual}`);
            continue;
        }

        // Detectar linha de COLABORADOR (código numérico)
        if (typeof valorA === 'number' || (!isNaN(parseInt(valorA)) && parseInt(valorA) > 0)) {
            const codigo = parseInt(valorA);

            // Extrair demais colunas pelos índices
            const getCellValue = (col) => {
                const cell = worksheet[xlsx.utils.encode_cell({ r: rowNum, c: col })];
                return cell ? cell.v : null;
            };

            const nome = getCellValue(4);
            const cargo = getCellValue(11);
            const centroCusto = getCellValue(18);  // C CUSTO RH - usado se PROJETO vazio no SQL
            const dataAdmissao = getCellValue(22);
            const situacao = getCellValue(26);
            const cpfRaw = getCellValue(28);

            // DEBUG: Log para identificar registros pulados
            if (nome && String(nome).toUpperCase().includes('WANDERLEY')) {
                console.log(`🔍 DEBUG WANDERLEY encontrado na linha ${rowNum}:`);
                console.log(`   - Nome: ${nome}`);
                console.log(`   - CPF: ${cpfRaw}`);
                console.log(`   - Situação: ${situacao} (tipo: ${typeof situacao})`);
                console.log(`   - Empresa: ${empresaAtual}`);
            }

            // Validações básicas
            if (!nome || !cpfRaw || !empresaAtual) {
                if (nome && String(nome).toUpperCase().includes('WANDERLEY')) {
                    console.log(`   ❌ Pulado: nome=${!!nome}, cpf=${!!cpfRaw}, empresa=${!!empresaAtual}`);
                }
                continue;
            }
            
            // Filtrar DEMITIDOS (situação 8) - não sobem para SQL
            const situacaoNum = parseInt(situacao);
            if (situacaoNum === 8) {
                continue; // Pula demitidos
            }

            const cpf = limparCPF(cpfRaw);
            if (!cpf || cpf.length !== 11) continue;

            const cargoNorm = normalizarCargo(cargo);
            const cnpj = EMPRESA_CNPJ[empresaAtual];
            const matricula = gerarMatricula(codigo, empresaAtual);
            const funcaoExecutante = obterFuncaoExecutante(cargoNorm);
            const classe = obterClasse(cargoNorm);

            // Extrair PROJETO do centro de custo (parte antes de "A")
            let projetoPlanilha = '';
            if (centroCusto) {
                const ccStr = String(centroCusto);
                const posA = ccStr.indexOf('A');
                projetoPlanilha = posA > 0 ? ccStr.substring(0, posA) : ccStr;
            }

            // Converter data do Excel (serial number) - CORRIGIDO FUSO HORÁRIO
            let dataAdmissaoDate = null;
            if (typeof dataAdmissao === 'number') {
                // Adiciona 12 horas para evitar problema de fuso horário
                dataAdmissaoDate = new Date((dataAdmissao - 25569) * 86400 * 1000 + 12 * 60 * 60 * 1000);
            } else if (dataAdmissao instanceof Date) {
                dataAdmissaoDate = dataAdmissao;
            }

            dados.push({
                NOME: nome.toUpperCase().trim(),
                FUNCAO: cargoNorm,
                CPF: cpf,
                DATA_ADMISSAO: dataAdmissaoDate,
                PROJETO_PLANILHA: projetoPlanilha,  // Usado se PROJETO vazio no SQL
                PROJETO_RH: centroCusto ? String(centroCusto).trim() : '',  // C. CUSTO completo
                SITUACAO: situacao !== null && situacao !== undefined ? String(situacao) : '',  // Valor original do Excel
                SITUACAO_TIPO: obterSituacaoTipo(situacao),  // Texto da situação
                // PROJETO, EQUIPE, COORDENADOR, SUPERVISOR, NOME_LIDER virão do SQL
                HORAS_TRABALHADAS: 8,
                FUNCAO_EXECUTANTE: funcaoExecutante,
                CLASSE: classe,
                ATUALIZADO_EM: new Date(),
                CNPJ: cnpj,
                EMPRESA: empresaAtual,
                MATRICULA: matricula
            });
        }
    }

    return dados;
}

// =============================================================================
// ROTAS DA API
// =============================================================================

app.post('/api/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'Nenhum arquivo enviado' });
        }

        console.log(`📁 Processando: ${req.file.originalname}`);

        const workbook = xlsx.readFile(req.file.path, { type: 'file', cellDates: true });
        
        console.log(`   📋 Planilhas encontradas: ${workbook.SheetNames.join(', ')}`);
        
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];

        if (!worksheet || !worksheet['!ref']) {
            fs.unlinkSync(req.file.path);
            return res.status(400).json({
                success: false,
                error: `Planilha "${sheetName}" está vazia ou não foi possível ler`
            });
        }

        console.log(`   📊 Range da planilha: ${worksheet['!ref']}`);

        const dados = processarPlanilhaTODOS(worksheet);

        fs.unlinkSync(req.file.path);

        if (dados.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Nenhum colaborador ativo encontrado na planilha'
            });
        }

        // Estatísticas
        const empresas = [...new Set(dados.map(d => d.EMPRESA))];

        // Preview (primeiros 10)
        const preview = dados.slice(0, 10).map(d => ({
            ...d,
            DATA_ADMISSAO: d.DATA_ADMISSAO ? d.DATA_ADMISSAO.toISOString().split('T')[0] : null
        }));

        console.log(`✅ ${dados.length} colaboradores processados`);

        // Armazenar em cache para uso no Excel (evita reenviar 2700 registros)
        cachedUploadData = dados;
        cacheTimestamp = Date.now();
        console.log(`💾 Dados em cache para geração de Excel`);

        res.json({
            success: true,
            preview,
            stats: {
                total: dados.length,
                valid: dados.length,
                invalid: 0,
                empresas: empresas
            },
            allValidRecords: dados
        });

    } catch (error) {
        console.error('❌ Erro no upload:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({
            success: false,
            error: 'Erro ao processar arquivo',
            details: error.message
        });
    }
});

app.post('/api/sync', async (req, res) => {
    const { records } = req.body;

    if (!records || !Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Nenhum registro para sincronizar' });
    }

    let pool = null;

    try {
        console.log(`🔄 Sincronizando ${records.length} registros...`);

        pool = await sql.connect(sqlConfig);
        
        // 1. BUSCAR dados existentes no SQL para preservar campos manuais
        const existingResult = await pool.request().query(`
            SELECT CPF, FUNCAO_EXECUTANTE, PROJETO, EQUIPE, COORDENADOR, SUPERVISOR, NOME_LIDER 
            FROM COLABORADORES
        `);
        
        // Criar mapa CPF -> dados do SQL
        const sqlDataMap = {};
        for (const row of existingResult.recordset) {
            sqlDataMap[row.CPF] = {
                FUNCAO_EXECUTANTE: row.FUNCAO_EXECUTANTE,
                PROJETO: row.PROJETO,
                EQUIPE: row.EQUIPE,
                COORDENADOR: row.COORDENADOR,
                SUPERVISOR: row.SUPERVISOR,
                NOME_LIDER: row.NOME_LIDER
            };
        }
        console.log(`📋 ${Object.keys(sqlDataMap).length} registros existentes no SQL`);

        // BEGIN TRANSACTION
        const transaction = new sql.Transaction(pool);
        await transaction.begin();

        try {
            // 2. DELETE FROM COLABORADORES (limpa tudo)
            await transaction.request().query('DELETE FROM COLABORADORES');
            console.log('🗑️  Tabela limpa');

            // 3. INSERT de todos os registros
            let inseridos = 0;
            let funcaoProtegida = 0;
            let camposPreservados = 0;
            const errors = [];

            for (const record of records) {
                try {
                    const dadosSQL = sqlDataMap[record.CPF];
                    
                    // FUNCAO_EXECUTANTE: Proteger se no SQL era MOTORISTA ou OPERADOR
                    let funcaoExecutanteFinal = record.FUNCAO_EXECUTANTE;
                    if (dadosSQL && dadosSQL.FUNCAO_EXECUTANTE) {
                        const funcaoSQLUpper = dadosSQL.FUNCAO_EXECUTANTE.toUpperCase();
                        if (funcaoSQLUpper.includes('MOTORISTA') || funcaoSQLUpper.includes('OPERADOR')) {
                            funcaoExecutanteFinal = dadosSQL.FUNCAO_EXECUTANTE;
                            funcaoProtegida++;
                        }
                    }

                    // PROJETO, EQUIPE, COORDENADOR, SUPERVISOR, NOME_LIDER: Manter do SQL
                    let projeto = '';
                    let equipe = '';
                    let coordenador = '';
                    let supervisor = '';
                    let nomeLider = '';
                    
                    if (dadosSQL) {
                        projeto = dadosSQL.PROJETO || '';
                        equipe = dadosSQL.EQUIPE || '';
                        coordenador = dadosSQL.COORDENADOR || '';
                        supervisor = dadosSQL.SUPERVISOR || '';
                        nomeLider = dadosSQL.NOME_LIDER || '';
                        if (projeto || equipe || coordenador || supervisor || nomeLider) {
                            camposPreservados++;
                        }
                    }

                    // Se PROJETO vazio no SQL, usa o CENTRO DE CUSTO da planilha
                    if (!projeto && record.PROJETO_PLANILHA) {
                        projeto = record.PROJETO_PLANILHA;
                    }

                    const request = new sql.Request(transaction);
                    
                    request.input('nome', sql.NVarChar(100), record.NOME);
                    request.input('funcao', sql.NVarChar(100), record.FUNCAO);
                    request.input('cpf', sql.VarChar(11), record.CPF);
                    request.input('dataAdmissao', sql.Date, record.DATA_ADMISSAO);
                    request.input('projeto', sql.VarChar(10), projeto);
                    request.input('equipe', sql.VarChar(20), equipe);
                    request.input('coordenador', sql.NVarChar(100), coordenador);
                    request.input('supervisor', sql.NVarChar(100), supervisor);
                    request.input('horas', sql.Int, record.HORAS_TRABALHADAS);
                    request.input('funcExec', sql.NVarChar(100), funcaoExecutanteFinal);
                    request.input('classe', sql.VarChar(10), record.CLASSE);
                    request.input('atualizado', sql.Date, record.ATUALIZADO_EM);
                    request.input('nomeLider', sql.VarChar(255), nomeLider);
                    request.input('cnpj', sql.VarChar(18), record.CNPJ);
                    request.input('empresa', sql.NVarChar(255), record.EMPRESA);
                    request.input('matricula', sql.VarChar(20), record.MATRICULA);
                    request.input('projetoRH', sql.NVarChar(100), record.PROJETO_RH || '');
                    request.input('situacao', sql.NVarChar(50), record.SITUACAO || '');
                    request.input('situacaoTipo', sql.NVarChar(100), record.SITUACAO_TIPO || '');

                    await request.query(`
                        INSERT INTO COLABORADORES (
                            NOME, FUNCAO, CPF, DATA_ADMISSAO, PROJETO, EQUIPE,
                            COORDENADOR, SUPERVISOR, HORAS_TRABALHADAS, FUNCAO_EXECUTANTE,
                            CLASSE, ATUALIZADO_EM, NOME_LIDER, CNPJ, EMPRESA, MATRICULA,
                            PROJETO_RH, SITUACAO, SITUACAO_TIPO
                        ) VALUES (
                            @nome, @funcao, @cpf, @dataAdmissao, @projeto, @equipe,
                            @coordenador, @supervisor, @horas, @funcExec,
                            @classe, @atualizado, @nomeLider, @cnpj, @empresa, @matricula,
                            @projetoRH, @situacao, @situacaoTipo
                        )
                    `);

                    inseridos++;

                } catch (recordError) {
                    errors.push({
                        cpf: record.CPF,
                        nome: record.NOME,
                        error: recordError.message
                    });
                }
            }

            // COMMIT
            await transaction.commit();
            console.log(`✅ ${inseridos} colaboradores inseridos`);
            console.log(`   📌 ${funcaoProtegida} com FUNCAO_EXECUTANTE protegida (MOTORISTA/OPERADOR)`);
            console.log(`   📌 ${camposPreservados} com campos manuais preservados`);

            res.json({
                success: true,
                message: 'Sincronização concluída com sucesso',
                results: {
                    total: records.length,
                    inserted: inseridos,
                    protectedFunctions: funcaoProtegida,
                    preservedFields: camposPreservados,
                    errors: errors.length,
                    errorDetails: errors
                }
            });

        } catch (error) {
            await transaction.rollback();
            throw error;
        }

    } catch (error) {
        console.error('❌ Erro na sincronização:', error);
        res.status(500).json({
            success: false,
            error: 'Erro ao sincronizar com banco de dados',
            details: error.message
        });
    } finally {
        if (pool) {
            try {
                await pool.close();
            } catch (e) {
                console.error('Erro ao fechar pool:', e);
            }
        }
    }
});

// Rota para gerar Excel PREVIEW (com dados mesclados do SQL) - Agora usa cache!
app.get('/api/download-excel', async (req, res) => {
    // Verificar se há dados em cache
    if (!cachedUploadData || !cacheTimestamp) {
        return res.status(400).json({ success: false, error: 'Faça upload do arquivo primeiro' });
    }

    // Verificar se o cache expirou
    if (Date.now() - cacheTimestamp > CACHE_DURATION) {
        cachedUploadData = null;
        cacheTimestamp = null;
        return res.status(400).json({ success: false, error: 'Cache expirou. Faça upload novamente.' });
    }

    const records = cachedUploadData;
    let pool = null;

    try {
        console.log(`📊 Gerando Excel com ${records.length} registros do cache...`);

        pool = await sql.connect(sqlConfig);
        
        // Buscar dados existentes no SQL
        const existingResult = await pool.request().query(`
            SELECT CPF, FUNCAO_EXECUTANTE, PROJETO, EQUIPE, COORDENADOR, SUPERVISOR, NOME_LIDER, HORAS_TRABALHADAS, CLASSE
            FROM COLABORADORES
        `);
        
        // Criar mapa CPF -> dados do SQL
        const sqlDataMap = {};
        for (const row of existingResult.recordset) {
            sqlDataMap[row.CPF] = row;
        }
        console.log(`📋 ${Object.keys(sqlDataMap).length} registros encontrados no SQL`);

        // Mesclar dados: Upload + SQL (aplicando as mesmas regras da sincronização)
        const mergedData = records.map(record => {
            const dadosSQL = sqlDataMap[record.CPF];
            
            // FUNCAO_EXECUTANTE: Proteger se no SQL era MOTORISTA ou OPERADOR
            let funcaoExecutanteFinal = record.FUNCAO_EXECUTANTE;
            if (dadosSQL && dadosSQL.FUNCAO_EXECUTANTE) {
                const funcaoSQLUpper = dadosSQL.FUNCAO_EXECUTANTE.toUpperCase();
                if (funcaoSQLUpper.includes('MOTORISTA') || funcaoSQLUpper.includes('OPERADOR')) {
                    funcaoExecutanteFinal = dadosSQL.FUNCAO_EXECUTANTE;
                }
            }

            // Campos do SQL (ou vazios se não existir)
            let projeto = '';
            let equipe = '';
            let coordenador = '';
            let supervisor = '';
            let nomeLider = '';
            let horasTrabalhadas = record.HORAS_TRABALHADAS || 8;
            
            if (dadosSQL) {
                projeto = dadosSQL.PROJETO || '';
                equipe = dadosSQL.EQUIPE || '';
                coordenador = dadosSQL.COORDENADOR || '';
                supervisor = dadosSQL.SUPERVISOR || '';
                nomeLider = dadosSQL.NOME_LIDER || '';
                horasTrabalhadas = dadosSQL.HORAS_TRABALHADAS || 8;
            }

            // Se PROJETO vazio no SQL, usa o CENTRO DE CUSTO da planilha
            if (!projeto && record.PROJETO_PLANILHA) {
                projeto = record.PROJETO_PLANILHA;
            }

            return {
                NOME: record.NOME,
                FUNCAO: record.FUNCAO,
                CPF: record.CPF,
                MATRICULA: record.MATRICULA,
                EMPRESA: record.EMPRESA,
                CNPJ: record.CNPJ,
                DATA_ADMISSAO: record.DATA_ADMISSAO ? new Date(record.DATA_ADMISSAO).toLocaleDateString('pt-BR') : '',
                PROJETO: projeto,
                PROJETO_RH: record.PROJETO_RH || '',
                SITUACAO: record.SITUACAO || '',
                SITUACAO_TIPO: record.SITUACAO_TIPO || '',
                EQUIPE: equipe,
                COORDENADOR: coordenador,
                SUPERVISOR: supervisor,
                HORAS_TRABALHADAS: horasTrabalhadas,
                FUNCAO_EXECUTANTE: funcaoExecutanteFinal,
                CLASSE: record.CLASSE,
                NOME_LIDER: nomeLider
            };
        });

        // Criar workbook e worksheet
        const wb = xlsx.utils.book_new();
        const ws = xlsx.utils.json_to_sheet(mergedData);

        // Ajustar largura das colunas
        ws['!cols'] = [
            { wch: 35 }, // NOME
            { wch: 30 }, // FUNCAO
            { wch: 14 }, // CPF
            { wch: 12 }, // MATRICULA
            { wch: 35 }, // EMPRESA
            { wch: 18 }, // CNPJ
            { wch: 12 }, // DATA_ADMISSAO
            { wch: 8 },  // PROJETO
            { wch: 25 }, // PROJETO_RH
            { wch: 10 }, // SITUACAO
            { wch: 45 }, // SITUACAO_TIPO
            { wch: 15 }, // EQUIPE
            { wch: 25 }, // COORDENADOR
            { wch: 25 }, // SUPERVISOR
            { wch: 8 },  // HORAS
            { wch: 20 }, // FUNCAO_EXECUTANTE
            { wch: 8 },  // CLASSE
            { wch: 25 }  // NOME_LIDER
        ];

        xlsx.utils.book_append_sheet(wb, ws, 'Colaboradores');

        // Gerar buffer do arquivo
        const buffer = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });

        // Enviar email com o Excel anexado (não-bloqueante)
        const dataHora = new Date().toLocaleString('pt-BR');
        const nomeArquivo = `colaboradores_${new Date().toISOString().split('T')[0]}.xlsx`;

        // Enviar email em background - não bloqueia o download
        enviarEmailAsync({
            from: process.env.EMAIL_USER || 'noreply@alrflorestal.com.br',
            to: EMAIL_DESTINATARIOS,
            subject: `📊 Relatório de Colaboradores - ${dataHora}`,
            html: `
                <h2>Relatório de Colaboradores</h2>
                <p>Segue em anexo o relatório de colaboradores gerado em <strong>${dataHora}</strong>.</p>
                <p><strong>Total de registros:</strong> ${mergedData.length}</p>
                <hr>
                <p style="color: #666; font-size: 12px;">Email enviado automaticamente pelo Sistema de Sincronização RH.</p>
            `,
            attachments: [
                {
                    filename: nomeArquivo,
                    content: buffer,
                    contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
            ]
        });

        // Enviar arquivo para download IMEDIATAMENTE
        res.setHeader('Content-Disposition', `attachment; filename=${nomeArquivo}`);
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.send(buffer);

        console.log('✅ Excel Preview gerado com sucesso');

    } catch (error) {
        console.error('❌ Erro ao gerar Excel Preview:', error);
        res.status(500).json({
            success: false,
            error: 'Erro ao gerar Excel',
            details: error.message
        });
    } finally {
        if (pool) {
            try {
                await pool.close();
            } catch (e) {
                console.error('Erro ao fechar pool:', e);
            }
        }
    }
});

// Rota para gerar Excel comparativo
app.post('/api/export-excel', (req, res) => {
    try {
        const { records } = req.body;

        if (!records || !Array.isArray(records) || records.length === 0) {
            return res.status(400).json({ success: false, error: 'Nenhum registro para exportar' });
        }

        console.log(`📊 Gerando Excel com ${records.length} registros...`);

        // Preparar dados para o Excel
        const excelData = records.map(r => ({
            NOME: r.NOME,
            FUNCAO: r.FUNCAO,
            CPF: r.CPF,
            DATA_ADMISSAO: r.DATA_ADMISSAO ? new Date(r.DATA_ADMISSAO).toLocaleDateString('pt-BR') : '',
            PROJETO: r.PROJETO,
            EQUIPE: r.EQUIPE,
            COORDENADOR: r.COORDENADOR || '',
            SUPERVISOR: r.SUPERVISOR || '',
            HORAS_TRABALHADAS: r.HORAS_TRABALHADAS,
            FUNCAO_EXECUTANTE: r.FUNCAO_EXECUTANTE,
            CLASSE: r.CLASSE,
            NOME_LIDER: r.NOME_LIDER || '',
            CNPJ: r.CNPJ,
            EMPRESA: r.EMPRESA,
            MATRICULA: r.MATRICULA
        }));

        // Criar workbook e worksheet
        const wb = xlsx.utils.book_new();
        const ws = xlsx.utils.json_to_sheet(excelData);

        // Ajustar largura das colunas
        ws['!cols'] = [
            { wch: 35 }, // NOME
            { wch: 30 }, // FUNCAO
            { wch: 14 }, // CPF
            { wch: 12 }, // DATA_ADMISSAO
            { wch: 8 },  // PROJETO
            { wch: 10 }, // EQUIPE
            { wch: 20 }, // COORDENADOR
            { wch: 20 }, // SUPERVISOR
            { wch: 8 },  // HORAS
            { wch: 20 }, // FUNCAO_EXECUTANTE
            { wch: 8 },  // CLASSE
            { wch: 25 }, // NOME_LIDER
            { wch: 18 }, // CNPJ
            { wch: 35 }, // EMPRESA
            { wch: 12 }  // MATRICULA
        ];

        xlsx.utils.book_append_sheet(wb, ws, 'Colaboradores');

        // Gerar buffer do arquivo
        const buffer = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });

        // Enviar arquivo
        res.setHeader('Content-Disposition', 'attachment; filename=colaboradores_comparativo.xlsx');
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.send(buffer);

        console.log('✅ Excel gerado com sucesso');

    } catch (error) {
        console.error('❌ Erro ao gerar Excel:', error);
        res.status(500).json({
            success: false,
            error: 'Erro ao gerar Excel',
            details: error.message
        });
    }
});

app.get('/api/health', async (req, res) => {
    let sqlStatus = 'disconnected';
    try {
        const pool = await sql.connect(sqlConfig);
        await pool.request().query('SELECT 1');
        sqlStatus = 'connected';
        await pool.close();
    } catch (error) {
        sqlStatus = `error: ${error.message}`;
    }
    res.json({ status: 'ok', timestamp: new Date().toISOString(), sql: sqlStatus });
});

// =============================================================================
// CRON JOB - BACKUP DIÁRIO PARA HISTÓRICO (23:50)
// =============================================================================

async function backupParaHistorico() {
    let pool = null;
    try {
        const dataHoje = new Date().toLocaleDateString('pt-BR');
        console.log(`⏰ [CRON] Iniciando backup diário para histórico - ${dataHoje}`);

        pool = await sql.connect(sqlConfig);

        // Copiar todos os registros de COLABORADORES para COLABORADORES_HISTORICO
        const result = await pool.request().query(`
            INSERT INTO COLABORADORES_HISTORICO (
                NOME, FUNCAO, CPF, DATA_ADMISSAO, PROJETO, PROJETO_RH,
                SITUACAO, SITUACAO_TIPO, EQUIPE, COORDENADOR, SUPERVISOR,
                HORAS_TRABALHADAS, FUNCAO_EXECUTANTE, CLASSE, NOME_LIDER,
                CNPJ, EMPRESA, MATRICULA, ATUALIZADO_EM, DATA_REGISTRO
            )
            SELECT 
                NOME, FUNCAO, CPF, DATA_ADMISSAO, PROJETO, PROJETO_RH,
                SITUACAO, SITUACAO_TIPO, EQUIPE, COORDENADOR, SUPERVISOR,
                HORAS_TRABALHADAS, FUNCAO_EXECUTANTE, CLASSE, NOME_LIDER,
                CNPJ, EMPRESA, MATRICULA, ATUALIZADO_EM, GETDATE()
            FROM COLABORADORES
        `);

        console.log(`✅ [CRON] Backup concluído! ${result.rowsAffected[0]} registros copiados para histórico`);

        // Enviar email de confirmação
        try {
            await emailTransporter.sendMail({
                from: process.env.EMAIL_USER || 'noreply@alrflorestal.com.br',
                to: EMAIL_DESTINATARIOS,
                subject: `✅ Backup Diário Colaboradores - ${dataHoje}`,
                html: `
                    <h2>Backup Diário Concluído</h2>
                    <p>O backup diário dos colaboradores foi realizado com sucesso.</p>
                    <p><strong>Data:</strong> ${dataHoje}</p>
                    <p><strong>Registros copiados:</strong> ${result.rowsAffected[0]}</p>
                    <hr>
                    <p style="color: #666; font-size: 12px;">Email automático do Sistema de Sincronização RH.</p>
                `
            });
            console.log(`📧 [CRON] Email de confirmação enviado`);
        } catch (emailError) {
            console.error(`❌ [CRON] Erro ao enviar email:`, emailError.message);
        }

    } catch (error) {
        console.error(`❌ [CRON] Erro no backup para histórico:`, error.message);
    } finally {
        if (pool) {
            try {
                await pool.close();
            } catch (e) {
                console.error('Erro ao fechar pool:', e);
            }
        }
    }
}

// Agendar CRON: todos os dias às 23:50 (horário do servidor)
// Formato: minuto hora dia mês dia-da-semana
cron.schedule('50 23 * * *', () => {
    backupParaHistorico();
}, {
    timezone: 'America/Sao_Paulo'
});

console.log('⏰ CRON agendado: Backup diário às 23:50 (Brasília)');

// Rota manual para executar o backup (para testes)
app.post('/api/backup-historico', async (req, res) => {
    try {
        await backupParaHistorico();
        res.json({ success: true, message: 'Backup executado com sucesso' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// =============================================================================
// INICIALIZAÇÃO
// =============================================================================

app.listen(PORT, () => {
    console.log('╔════════════════════════════════════════════════════╗');
    console.log('║   SISTEMA DE SINCRONIZAÇÃO RH - ATIVOS            ║');
    console.log('╠════════════════════════════════════════════════════╣');
    console.log(`║  🚀 Servidor: http://localhost:${PORT.toString().padEnd(25)} ║`);
    console.log('║  📊 Endpoint: POST /api/upload                     ║');
    console.log('║  🔄 Endpoint: POST /api/sync                       ║');
    console.log('║  ⏰ CRON: Backup histórico às 23:50                ║');
    console.log('╚════════════════════════════════════════════════════╝');
});

module.exports = app;