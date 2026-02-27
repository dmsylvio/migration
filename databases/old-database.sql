-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: brasilia-estagios-db_brasilia_estagios_mariadb:3306
-- Tempo de geração: 27-Jan-2026 às 03:15
-- Versão do servidor: 11.8.5-MariaDB-ubu2404
-- versão do PHP: 8.2.27

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Banco de dados: `brasilia-estagios-db`
--

-- --------------------------------------------------------

--
-- Estrutura da tabela `prorrogacao_de_contrato`
--

CREATE TABLE `prorrogacao_de_contrato` (
  `id` int(11) NOT NULL,
  `empresa_id` int(11) NOT NULL,
  `nome_fantasia` varchar(255) NOT NULL,
  `telefone` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `responsavel` varchar(255) NOT NULL,
  `data_solicitacao` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `aditivo_especificar` text NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `updated_at` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `isDeleted` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `recibo_pagamento_bolsa`
--

CREATE TABLE `recibo_pagamento_bolsa` (
  `id` char(36) NOT NULL,
  `empresa_id` char(36) NOT NULL,
  `estudante_id` char(36) NOT NULL,
  `valor` decimal(10,2) DEFAULT NULL,
  `data_inicio` date DEFAULT NULL,
  `data_fim` date DEFAULT NULL,
  `data_pagamento` date DEFAULT curdate(),
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `is_deleted` tinyint(1) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `recibo_recesso_remunerado`
--

CREATE TABLE `recibo_recesso_remunerado` (
  `id` char(36) NOT NULL,
  `termo_id` char(36) NOT NULL,
  `empresa_id` char(36) NOT NULL,
  `estudante_id` char(36) NOT NULL,
  `valor` decimal(10,2) DEFAULT NULL,
  `data_inicio_recesso` date DEFAULT NULL,
  `data_fim_recesso` date DEFAULT NULL,
  `data_pagamento` date DEFAULT curdate(),
  `created_at` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `updated_at` datetime(3) NOT NULL DEFAULT current_timestamp(3) ON UPDATE current_timestamp(3),
  `is_deleted` tinyint(1) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `representante_empresas`
--

CREATE TABLE `representante_empresas` (
  `id` int(11) NOT NULL,
  `empresa_id` char(36) NOT NULL,
  `nomeCompleto` varchar(255) NOT NULL,
  `cpf` varchar(14) NOT NULL,
  `rg` varchar(40) DEFAULT NULL,
  `orgaoEmissor` varchar(60) DEFAULT NULL,
  `telefone` varchar(15) DEFAULT NULL,
  `celular` varchar(15) DEFAULT NULL,
  `cargo` varchar(60) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `representante_instituicaos`
--

CREATE TABLE `representante_instituicaos` (
  `instituicao_id` char(36) NOT NULL,
  `nomeCompleto` varchar(255) NOT NULL,
  `cpf` varchar(14) NOT NULL,
  `rg` varchar(40) DEFAULT NULL,
  `orgaoEmissor` varchar(60) DEFAULT NULL,
  `telefone` varchar(15) DEFAULT NULL,
  `celular` varchar(15) DEFAULT NULL,
  `cargo` varchar(60) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `solicitar_estagiarios`
--

CREATE TABLE `solicitar_estagiarios` (
  `id` uuid NOT NULL DEFAULT uuid(),
  `empresa_id` char(36) NOT NULL,
  `endereco` varchar(191) NOT NULL,
  `cidade` varchar(191) NOT NULL,
  `telefone_empresa` varchar(16) DEFAULT NULL,
  `email` varchar(191) NOT NULL,
  `responsavel` varchar(191) NOT NULL,
  `hora_entrevista` varchar(191) NOT NULL,
  `curso_id` int(11) NOT NULL,
  `semestre_id` int(11) NOT NULL,
  `numero_vagas` int(11) NOT NULL DEFAULT 1,
  `genero` enum('Masculino','Feminino','Outro') NOT NULL DEFAULT 'Outro',
  `horario_estagio` varchar(191) NOT NULL,
  `valor_bolsa_auxilio` varchar(191) NOT NULL,
  `valor_vale_transporte` varchar(191) DEFAULT NULL,
  `valor_vale_alimentacao` varchar(191) DEFAULT NULL,
  `conhecimentos_exigidos` text DEFAULT NULL,
  `atividades_serem_realizadas` text DEFAULT NULL,
  `status_solicitacao` enum('pendente','concluido') NOT NULL DEFAULT 'pendente',
  `data_solicitacao` datetime NOT NULL DEFAULT current_timestamp(),
  `created_at` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `updated_at` datetime(3) NOT NULL DEFAULT current_timestamp(3) ON UPDATE current_timestamp(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `solicitar_rescisao_termos`
--

CREATE TABLE `solicitar_rescisao_termos` (
  `id` uuid NOT NULL DEFAULT uuid(),
  `termo_id` char(36) DEFAULT NULL,
  `empresa_id` char(36) DEFAULT NULL,
  `estudante_id` int(11) NOT NULL,
  `data_rescisao` date DEFAULT NULL,
  `data_solicitacao` date NOT NULL DEFAULT current_timestamp(3),
  `motivo_rescisao` enum('TerminoAutomatico','IniciativaEstagiario','IniciativaEmpresa','NaoAssumiuVaga','ConclusaoAbandonoTrancamento','EfetivadoEmpresa','DescumprimentoContratual','AusenciaInjustificada','Outros') NOT NULL,
  `status_solicitacao` enum('pendente','concluido') NOT NULL DEFAULT 'pendente',
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `solicitar_termos`
--

CREATE TABLE `solicitar_termos` (
  `id` uuid NOT NULL DEFAULT uuid(),
  `empresa_id` varchar(36) NOT NULL,
  `representante_empresa` varchar(191) NOT NULL,
  `supervisor_empresa` varchar(191) NOT NULL,
  `telefone_empresa` varchar(16) DEFAULT NULL,
  `email_empresa` varchar(191) NOT NULL,
  `email_supervisor` varchar(191) DEFAULT NULL,
  `numero_conselho` varchar(191) DEFAULT NULL,
  `nome_estudante` varchar(191) NOT NULL,
  `telefone_estudante` varchar(16) DEFAULT NULL,
  `curso` varchar(191) NOT NULL,
  `semestre` varchar(191) NOT NULL,
  `data_inicio_estagio` date NOT NULL,
  `vigencia_estagio` int(11) NOT NULL,
  `horario_estagio` varchar(191) NOT NULL,
  `valor_bolsa_auxilio` decimal(10,2) NOT NULL,
  `valor_vale_transporte` decimal(10,2) DEFAULT NULL,
  `outros_beneficios` text DEFAULT NULL,
  `atividades_realizadas` text NOT NULL,
  `data_solicitacao` datetime NOT NULL DEFAULT current_timestamp(),
  `status_solicitacao` enum('pendente','concluido') NOT NULL DEFAULT 'pendente',
  `created_at` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `updated_at` datetime(3) NOT NULL DEFAULT current_timestamp(3) ON UPDATE current_timestamp(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `supervisor_empresas`
--

CREATE TABLE `supervisor_empresas` (
  `empresa_id` char(36) NOT NULL,
  `nomeCompleto` varchar(255) NOT NULL,
  `cpf` varchar(14) NOT NULL,
  `rg` varchar(40) DEFAULT NULL,
  `orgaoEmissor` varchar(60) DEFAULT NULL,
  `telefone` varchar(15) DEFAULT NULL,
  `celular` varchar(15) DEFAULT NULL,
  `cargo` varchar(60) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `supervisor_instituicaos`
--

CREATE TABLE `supervisor_instituicaos` (
  `instituicao_id` char(36) NOT NULL,
  `nomeCompleto` varchar(255) NOT NULL,
  `cpf` varchar(14) NOT NULL,
  `rg` varchar(40) DEFAULT NULL,
  `orgaoEmissor` varchar(60) DEFAULT NULL,
  `telefone` varchar(15) DEFAULT NULL,
  `celular` varchar(15) DEFAULT NULL,
  `cargo` varchar(60) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_categoria`
--

CREATE TABLE `tb_categoria` (
  `id` int(11) NOT NULL,
  `categoria` varchar(255) NOT NULL,
  `libera` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_confcurso`
--

CREATE TABLE `tb_confcurso` (
  `co_seq_confcurso` int(11) NOT NULL,
  `co_usuario` int(11) NOT NULL DEFAULT 0,
  `dt_publicado` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `no_curso` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_confinstituicao`
--

CREATE TABLE `tb_confinstituicao` (
  `co_seq_confinstituicao` int(10) NOT NULL,
  `co_usuario` int(10) DEFAULT 0,
  `dt_publicado` date NOT NULL DEFAULT '0000-00-00',
  `no_instituicao` text CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_cursoinfnew`
--

CREATE TABLE `tb_cursoinfnew` (
  `co_seq_cursoinfnew` int(10) NOT NULL,
  `co_estudante` int(10) NOT NULL DEFAULT 0,
  `ds_cursoinfnew` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_dados_financeiros`
--

CREATE TABLE `tb_dados_financeiros` (
  `id` int(11) NOT NULL,
  `empresa_id` int(11) NOT NULL,
  `responsavel` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `whatsapp` varchar(255) NOT NULL,
  `vencimento` varchar(2) NOT NULL,
  `valor` text NOT NULL,
  `created_at` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_depoimento`
--

CREATE TABLE `tb_depoimento` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `description` varchar(255) NOT NULL,
  `testimonial` longtext NOT NULL,
  `image` varchar(255) NOT NULL,
  `data` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `status` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_empresa`
--

CREATE TABLE `tb_empresa` (
  `co_seq_empresa` int(10) NOT NULL,
  `id` char(36) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `st_habilitado` int(1) NOT NULL DEFAULT 0,
  `ds_razao_social` varchar(191) NOT NULL,
  `ds_nome_fantasia` varchar(191) NOT NULL,
  `nu_cnpj` varchar(18) NOT NULL,
  `ds_insc_est` varchar(100) DEFAULT '',
  `ds_endereco` varchar(191) NOT NULL,
  `ds_cidade` varchar(100) NOT NULL,
  `ds_uf` varchar(2) NOT NULL DEFAULT '',
  `nu_cep` varchar(10) DEFAULT '0',
  `nu_telefone` varchar(16) NOT NULL,
  `nu_fax` varchar(16) DEFAULT '0',
  `ds_email` varchar(100) DEFAULT '',
  `ds_atividade` varchar(100) DEFAULT '',
  `dt_cadastro` date NOT NULL DEFAULT curdate(),
  `ds_obs_futura_emp` mediumtext DEFAULT NULL,
  `ds_email2` varchar(100) DEFAULT '',
  `ds_email3` varchar(100) DEFAULT '',
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_escolaridade`
--

CREATE TABLE `tb_escolaridade` (
  `id` int(11) NOT NULL,
  `nivel` varchar(30) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_estados`
--

CREATE TABLE `tb_estados` (
  `id` int(11) NOT NULL,
  `estado` varchar(40) NOT NULL,
  `uf` varchar(2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_estado_civil`
--

CREATE TABLE `tb_estado_civil` (
  `id` int(11) NOT NULL,
  `estado_civil` varchar(255) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_estudante`
--

CREATE TABLE `tb_estudante` (
  `id` int(11) NOT NULL,
  `usuario_id` int(11) NOT NULL,
  `notas` text DEFAULT NULL,
  `nome_completo` varchar(191) NOT NULL,
  `data_nascimento` date NOT NULL,
  `cpf` varchar(14) NOT NULL,
  `rg` varchar(191) NOT NULL,
  `orgao_expedidor` varchar(191) DEFAULT NULL,
  `possui_cnh` tinyint(1) NOT NULL DEFAULT 0,
  `genero` enum('masculino','feminino','ambos') NOT NULL DEFAULT 'masculino',
  `estado_civil_id` int(11) NOT NULL,
  `possui_deficiencia` tinyint(1) NOT NULL DEFAULT 0,
  `tipo_deficiencia` varchar(191) DEFAULT NULL,
  `nome_pai` varchar(191) DEFAULT NULL,
  `nome_mae` varchar(191) DEFAULT NULL,
  `endereco` varchar(191) DEFAULT NULL,
  `bairro` varchar(191) DEFAULT NULL,
  `cidade` varchar(100) DEFAULT NULL,
  `estado_id` int(11) NOT NULL,
  `cep` varchar(9) NOT NULL,
  `email` varchar(100) DEFAULT NULL,
  `telefone` varchar(20) NOT NULL,
  `whatsapp` varchar(20) DEFAULT NULL,
  `nivel_escolaridade_id` int(11) NOT NULL,
  `curso_id` int(11) NOT NULL,
  `instituicao_id` int(11) NOT NULL,
  `possui_oab` int(1) DEFAULT NULL,
  `matricula` varchar(100) DEFAULT NULL,
  `semestre_id` int(11) DEFAULT NULL,
  `turno_id` int(11) NOT NULL,
  `horario_disponivel` enum('matutino','vespertino','noturno','tempo_integral') NOT NULL DEFAULT 'matutino',
  `ingles` enum('nenhum','básico','intermediário','avançado') NOT NULL DEFAULT 'nenhum',
  `frances` enum('nenhum','básico','intermediário','avançado') NOT NULL DEFAULT 'nenhum',
  `espanhol` enum('nenhum','básico','intermediário','avançado') NOT NULL DEFAULT 'nenhum',
  `outro_idioma` varchar(255) DEFAULT NULL,
  `OtherQualifications` varchar(250) DEFAULT NULL,
  `ImprovementCourse` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `ITCourse` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `createdAt` datetime DEFAULT current_timestamp(),
  `updatedAt` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_experprofis`
--

CREATE TABLE `tb_experprofis` (
  `co_seq_experprofis` int(10) NOT NULL,
  `co_estudante` int(10) NOT NULL DEFAULT 0,
  `ds_experprofis` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_instituicao`
--

CREATE TABLE `tb_instituicao` (
  `co_seq_instituicao` int(10) NOT NULL,
  `id` char(36) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `st_habilitado` int(1) NOT NULL DEFAULT 0,
  `ds_razao_social` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_nome_fantasia` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `nu_cnpj` varchar(18) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '0',
  `ds_insc_est` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_endereco` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_cidade` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_uf` char(2) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `nu_cep` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '0',
  `nu_telefone` varchar(16) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '0',
  `nu_fax` varchar(16) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '0',
  `ds_email` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_atividade` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_obsfutura` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `dt_cadastro` datetime DEFAULT curdate(),
  `ds_obs_futura_inst` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `ds_email2` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_email3` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_noticia`
--

CREATE TABLE `tb_noticia` (
  `id` int(11) NOT NULL,
  `chapeu` varchar(70) NOT NULL,
  `titulo` varchar(255) NOT NULL,
  `subtitulo` varchar(255) DEFAULT NULL,
  `texto` longtext NOT NULL,
  `imagem1` varchar(255) NOT NULL,
  `imagem1_legenda` varchar(70) DEFAULT NULL,
  `imagem1_credito` varchar(70) DEFAULT NULL,
  `libera_comentario` tinyint(1) NOT NULL DEFAULT 1,
  `libera` tinyint(1) NOT NULL DEFAULT 1,
  `id_categoria` int(11) NOT NULL,
  `url_curta` varchar(255) NOT NULL,
  `id_user` int(11) NOT NULL,
  `autor` varchar(255) NOT NULL,
  `data` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_representante`
--

CREATE TABLE `tb_representante` (
  `co_seq_representante` int(10) NOT NULL,
  `co_empresa` int(11) DEFAULT NULL,
  `co_instituicao` int(11) DEFAULT NULL,
  `no_representante` varchar(255) DEFAULT '',
  `email` varchar(255) DEFAULT NULL,
  `nu_rg` bigint(15) DEFAULT 0,
  `ds_orgao` varchar(100) DEFAULT '',
  `nu_cpf` varchar(14) DEFAULT '0',
  `nu_celular` varchar(16) DEFAULT '0',
  `ds_cargo` varchar(255) DEFAULT '',
  `tp_representante` char(1) DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_semestre`
--

CREATE TABLE `tb_semestre` (
  `id` int(11) NOT NULL,
  `semestre` varchar(30) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_sexo`
--

CREATE TABLE `tb_sexo` (
  `id` int(11) NOT NULL,
  `sexo` varchar(255) NOT NULL,
  `code` varchar(1) DEFAULT NULL,
  `created_at` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_supervisor`
--

CREATE TABLE `tb_supervisor` (
  `co_seq_supervisor` int(11) NOT NULL,
  `no_supervisor` varchar(255) DEFAULT '',
  `nu_rgsup` varchar(15) DEFAULT '0',
  `ds_orgao_expsup` varchar(100) DEFAULT '',
  `nu_cpfsup` varchar(14) DEFAULT '0',
  `nu_celularsup` varchar(16) DEFAULT '0',
  `ds_cargosup` varchar(255) DEFAULT '',
  `tp_entidade` char(1) DEFAULT '',
  `co_empresa` int(10) DEFAULT 0,
  `co_instituicao` int(10) DEFAULT 0
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_termo`
--

CREATE TABLE `tb_termo` (
  `co_seq_termo` int(10) NOT NULL,
  `id` char(36) NOT NULL,
  `notas` mediumtext DEFAULT NULL,
  `usuario_id` char(36) NOT NULL,
  `empresa_id` char(36) NOT NULL,
  `supervisor_empresa_id` int(11) DEFAULT NULL,
  `supervisor_empresa` varchar(255) NOT NULL,
  `cargo_supervisor_empresa` varchar(255) DEFAULT NULL,
  `representante_empresa_id` int(11) DEFAULT NULL,
  `representante_empresa` varchar(255) DEFAULT NULL,
  `cargo_representante_empresa` varchar(255) DEFAULT NULL,
  `instituicao_id` char(36) NOT NULL,
  `supervisor_instituicao_id` int(11) DEFAULT NULL,
  `supervisor_instituicao` varchar(255) NOT NULL,
  `cargo_supervisor_instituicao` varchar(255) DEFAULT NULL,
  `representante_instituicao_id` int(11) DEFAULT NULL,
  `representante_instituicao` varchar(255) NOT NULL,
  `cargo_representante_instituicao` varchar(255) DEFAULT NULL,
  `estudante_id` int(11) NOT NULL,
  `data` date DEFAULT current_timestamp(),
  `paragrafo_a` mediumtext DEFAULT NULL,
  `paragrafo_b` mediumtext DEFAULT NULL,
  `data_inicio` date DEFAULT NULL,
  `data_fim` date DEFAULT NULL,
  `hora_especial` mediumtext DEFAULT NULL,
  `valor_estagio` varchar(191) DEFAULT NULL,
  `old_taxa_pagamento` varchar(100) DEFAULT NULL,
  `taxa_pagamento` enum('Hora','Diario','Semanalmente','Mensalmente') NOT NULL DEFAULT 'Mensalmente',
  `vale_transporte` varchar(191) DEFAULT NULL,
  `co_usuario` int(10) DEFAULT 0,
  `apoliceseguro` varchar(191) NOT NULL DEFAULT '-316',
  `seguradora` mediumtext DEFAULT NULL,
  `prorrogacao1` date DEFAULT NULL,
  `prorrogacao2` date DEFAULT NULL,
  `prorrogacao3` date DEFAULT NULL,
  `rescisao` date DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_turno`
--

CREATE TABLE `tb_turno` (
  `id` int(11) NOT NULL,
  `turno` varchar(255) NOT NULL,
  `code` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_usuario`
--

CREATE TABLE `tb_usuario` (
  `co_seq_usuario` int(11) NOT NULL,
  `id` char(36) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `email` varchar(255) NOT NULL,
  `auth_key` varchar(32) DEFAULT NULL,
  `password_hash` varchar(255) DEFAULT NULL,
  `password_reset_token` varchar(255) DEFAULT 'NULL',
  `verification_token` varchar(255) DEFAULT 'NULL',
  `image` varchar(255) DEFAULT NULL,
  `role` enum('undefined','student','company','institution','admin') NOT NULL DEFAULT 'undefined',
  `google_id` varchar(255) DEFAULT NULL,
  `auth_type` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `co_empresa` int(11) DEFAULT NULL,
  `co_instituicao` int(11) DEFAULT NULL,
  `co_estudante` int(11) DEFAULT NULL,
  `ds_login` varchar(191) DEFAULT NULL,
  `ds_senha` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tb_vaga`
--

CREATE TABLE `tb_vaga` (
  `co_seq_vaga` int(10) NOT NULL,
  `status` enum('Aberto','Fechado') NOT NULL DEFAULT 'Aberto',
  `co_usuario` int(10) NOT NULL DEFAULT 0,
  `co_empresa` int(10) NOT NULL DEFAULT 0,
  `co_confcurso` int(10) DEFAULT 0,
  `nu_vaga` int(20) DEFAULT NULL,
  `nu_qtvaga` int(10) DEFAULT NULL,
  `dt_abertura` date NOT NULL DEFAULT '0000-00-00',
  `ds_supervisor` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `ds_contato` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_pontoreferencia` longtext CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `ds_diahorario` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `no_entrevistador` varchar(250) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_nivel` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_semestre` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `tp_sexo` char(1) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `st_deficiencia` int(1) DEFAULT 0,
  `ds_horario` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `vl_bolsa` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '0',
  `ds_beneficio` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT '',
  `ds_exigencia` longtext CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `ds_atividade` longtext CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `interessados` longtext CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `ds_observacao` longtext CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `st_disponivel` int(11) NOT NULL DEFAULT 1,
  `st_habilitado` int(11) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `tce_docs`
--

CREATE TABLE `tce_docs` (
  `id` int(11) NOT NULL,
  `estudante_id` int(11) NOT NULL,
  `empresa_id` int(11) NOT NULL,
  `tce_id` int(11) NOT NULL,
  `documento` varchar(255) NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `termos`
--

CREATE TABLE `termos` (
  `id` int(11) NOT NULL,
  `userId` int(11) NOT NULL,
  `notas` longtext DEFAULT NULL,
  `empresaId` int(11) NOT NULL,
  `representanteEmpresaId` int(11) DEFAULT NULL,
  `cargoRepresentanteEmpresa` varchar(191) DEFAULT NULL,
  `supervisorEmpresaId` int(11) DEFAULT NULL,
  `cargoSupervisorEmpresa` varchar(191) DEFAULT NULL,
  `instituicaoId` int(11) NOT NULL,
  `representanteInstituicaoId` int(11) DEFAULT NULL,
  `cargoRepresentanteInstituicao` varchar(191) DEFAULT NULL,
  `supervisorInstituicaoId` int(11) DEFAULT NULL,
  `cargoSupervisorInstituicao` varchar(191) DEFAULT NULL,
  `estudanteId` int(11) NOT NULL,
  `paragrafoA` longtext NOT NULL,
  `paragrafoB` longtext NOT NULL,
  `dataInicialEstagio` date NOT NULL,
  `dataFinalEstagio` date DEFAULT NULL,
  `diasHorarios` varchar(191) NOT NULL,
  `valorBolsa` varchar(191) NOT NULL,
  `frequenciaPagamento` enum('Hora','Diario','Semanal','Mensal') NOT NULL DEFAULT 'Mensal',
  `valorAuxilioTransporte` varchar(191) NOT NULL,
  `dataTermo` date NOT NULL,
  `primeiraProrrogacao` date DEFAULT NULL,
  `segundaProrrogacao` date DEFAULT NULL,
  `terceiraProrrogacao` date DEFAULT NULL,
  `dataRescisao` date DEFAULT NULL,
  `createdAt` datetime(3) NOT NULL DEFAULT current_timestamp(3),
  `updatedAt` datetime(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `termo_realizacao_estagios`
--

CREATE TABLE `termo_realizacao_estagios` (
  `id` uuid NOT NULL DEFAULT uuid(),
  `empresa_id` varchar(36) NOT NULL,
  `estudante_id` int(11) NOT NULL,
  `data_inicio` date DEFAULT NULL,
  `data_fim` date DEFAULT NULL,
  `total_horas` int(11) DEFAULT NULL,
  `atividades` text DEFAULT NULL,
  `avaliacao_pessoal` text DEFAULT NULL,
  `avaliacao_profissional` text DEFAULT NULL,
  `avaliacao_responsabilidade` text DEFAULT NULL,
  `avaliacao_tecnica` text DEFAULT NULL,
  `data_avaliacao` date DEFAULT NULL,
  `supervisor_assinatura` varchar(255) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estrutura da tabela `vagas`
--

CREATE TABLE `vagas` (
  `id` char(36) NOT NULL,
  `codigo_vaga` int(11) NOT NULL,
  `status` enum('Visivel','Aberta','Fechada','Cancelada') NOT NULL DEFAULT 'Aberta',
  `user_id` char(36) NOT NULL,
  `empresa_id` char(36) NOT NULL,
  `curso` varchar(255) NOT NULL,
  `qt_vaga` int(11) DEFAULT NULL,
  `supervisor` varchar(255) DEFAULT NULL,
  `contato` varchar(200) DEFAULT '',
  `ponto_referencia` text DEFAULT NULL,
  `dia_horario` varchar(200) DEFAULT '',
  `entrevistador` varchar(250) DEFAULT '',
  `nivel` varchar(200) DEFAULT '',
  `semestre` varchar(100) DEFAULT NULL,
  `sexo` varchar(50) DEFAULT NULL,
  `deficiencia` tinyint(1) NOT NULL DEFAULT 0,
  `horario` varchar(200) DEFAULT '',
  `bolsa` varchar(100) DEFAULT '0',
  `beneficio` varchar(100) DEFAULT '',
  `exigencia` text DEFAULT NULL,
  `atividade` text DEFAULT NULL,
  `interessados` text NOT NULL,
  `observacao` text DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Índices para tabelas despejadas
--

--
-- Índices para tabela `prorrogacao_de_contrato`
--
ALTER TABLE `prorrogacao_de_contrato`
  ADD PRIMARY KEY (`id`),
  ADD KEY `prorrogacao_de_contrato_tb_empresa_co_seq_empresa_fk` (`empresa_id`);

--
-- Índices para tabela `recibo_pagamento_bolsa`
--
ALTER TABLE `recibo_pagamento_bolsa`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `recibo_recesso_remunerado`
--
ALTER TABLE `recibo_recesso_remunerado`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `representante_empresas`
--
ALTER TABLE `representante_empresas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `empresa_id` (`empresa_id`);

--
-- Índices para tabela `representante_instituicaos`
--
ALTER TABLE `representante_instituicaos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `instituicao_id` (`instituicao_id`);

--
-- Índices para tabela `solicitar_estagiarios`
--
ALTER TABLE `solicitar_estagiarios`
  ADD PRIMARY KEY (`id`),
  ADD KEY `solicitar_estagiarios_tb_empresa_id_fk` (`empresa_id`),
  ADD KEY `solicitar_estagiarios_tb_confcurso_co_seq_confcurso_fk` (`curso_id`),
  ADD KEY `solicitar_estagiarios_tb_semestre_id_fk` (`semestre_id`);

--
-- Índices para tabela `solicitar_rescisao_termos`
--
ALTER TABLE `solicitar_rescisao_termos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `solicitar_rescisao_termos_tb_termo_id_fk` (`termo_id`),
  ADD KEY `solicitar_rescisao_termos_tb_empresa_id_fk` (`empresa_id`),
  ADD KEY `solicitar_rescisao_termos_tb_estudante_id_fk` (`estudante_id`);

--
-- Índices para tabela `solicitar_termos`
--
ALTER TABLE `solicitar_termos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `solicitar_termos_tb_empresa_id_fk` (`empresa_id`);

--
-- Índices para tabela `supervisor_empresas`
--
ALTER TABLE `supervisor_empresas`
  ADD PRIMARY KEY (`id`),
  ADD KEY `empresa_id` (`empresa_id`);

--
-- Índices para tabela `supervisor_instituicaos`
--
ALTER TABLE `supervisor_instituicaos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `institution_id` (`instituicao_id`);

--
-- Índices para tabela `tb_categoria`
--
ALTER TABLE `tb_categoria`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `tb_confcurso`
--
ALTER TABLE `tb_confcurso`
  ADD PRIMARY KEY (`co_seq_confcurso`);

--
-- Índices para tabela `tb_confinstituicao`
--
ALTER TABLE `tb_confinstituicao`
  ADD PRIMARY KEY (`co_seq_confinstituicao`);

--
-- Índices para tabela `tb_cursoinfnew`
--
ALTER TABLE `tb_cursoinfnew`
  ADD PRIMARY KEY (`co_seq_cursoinfnew`,`co_estudante`);

--
-- Índices para tabela `tb_dados_financeiros`
--
ALTER TABLE `tb_dados_financeiros`
  ADD PRIMARY KEY (`id`),
  ADD KEY `dados_financeiros_tb_empresa_co_seq_empresa_fk` (`empresa_id`);

--
-- Índices para tabela `tb_depoimento`
--
ALTER TABLE `tb_depoimento`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `tb_empresa`
--
ALTER TABLE `tb_empresa`
  ADD PRIMARY KEY (`co_seq_empresa`),
  ADD UNIQUE KEY `user_id` (`user_id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `ds_razao_social_INDEX` (`ds_razao_social`),
  ADD KEY `ds_nome_fantasia_INDEX` (`ds_nome_fantasia`),
  ADD KEY `ds_razao_social_ds_nome_fantasia_INDEX` (`ds_razao_social`,`ds_nome_fantasia`),
  ADD KEY `dt_cadastro_INDEX` (`dt_cadastro`),
  ADD KEY `st_habilitado_INDEX` (`st_habilitado`),
  ADD KEY `idx_co_seq_empresa` (`co_seq_empresa`),
  ADD KEY `id_2` (`id`),
  ADD KEY `idx_empresa_user_id` (`user_id`);

--
-- Índices para tabela `tb_escolaridade`
--
ALTER TABLE `tb_escolaridade`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `tb_escolaridade_id_uindex` (`id`),
  ADD UNIQUE KEY `tb_escolaridade_nivel_uindex` (`nivel`);

--
-- Índices para tabela `tb_estados`
--
ALTER TABLE `tb_estados`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `tb_estados_id_uindex` (`id`),
  ADD UNIQUE KEY `tb_estados_estado_uindex` (`estado`),
  ADD UNIQUE KEY `tb_estados_uf_uindex` (`uf`);

--
-- Índices para tabela `tb_estado_civil`
--
ALTER TABLE `tb_estado_civil`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `tb_estudante`
--
ALTER TABLE `tb_estudante`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_usuario_id` (`usuario_id`),
  ADD KEY `tb_estudante_tb_estado_civil_id_fk` (`estado_civil_id`),
  ADD KEY `tb_estudante_tb_estados_id_fk` (`estado_id`),
  ADD KEY `tb_estudante_tb_escolaridade_id_fk` (`nivel_escolaridade_id`),
  ADD KEY `tb_estudante_tb_confcurso_co_seq_confcurso_fk` (`curso_id`),
  ADD KEY `tb_estudante_tb_confinstituicao_co_seq_confinstituicao_fk` (`instituicao_id`),
  ADD KEY `tb_estudante_tb_semestre_id_fk` (`semestre_id`),
  ADD KEY `tb_estudante_tb_turno_id_fk` (`turno_id`);

--
-- Índices para tabela `tb_experprofis`
--
ALTER TABLE `tb_experprofis`
  ADD PRIMARY KEY (`co_seq_experprofis`,`co_estudante`);

--
-- Índices para tabela `tb_instituicao`
--
ALTER TABLE `tb_instituicao`
  ADD PRIMARY KEY (`co_seq_instituicao`),
  ADD UNIQUE KEY `idx_user_id_unique` (`user_id`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `idx_instituicao_user_id` (`user_id`);

--
-- Índices para tabela `tb_noticia`
--
ALTER TABLE `tb_noticia`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `tb_noticia_id_uindex` (`id`),
  ADD KEY `tb_noticia_tb_categoria_id_fk` (`id_categoria`);

--
-- Índices para tabela `tb_representante`
--
ALTER TABLE `tb_representante`
  ADD PRIMARY KEY (`co_seq_representante`);

--
-- Índices para tabela `tb_semestre`
--
ALTER TABLE `tb_semestre`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `tb_semestre_id_uindex` (`id`),
  ADD UNIQUE KEY `tb_semestre_semestre_uindex` (`semestre`);

--
-- Índices para tabela `tb_sexo`
--
ALTER TABLE `tb_sexo`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `tb_supervisor`
--
ALTER TABLE `tb_supervisor`
  ADD PRIMARY KEY (`co_seq_supervisor`);

--
-- Índices para tabela `tb_termo`
--
ALTER TABLE `tb_termo`
  ADD PRIMARY KEY (`co_seq_termo`),
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `fk_tb_termo_instituicao_id` (`instituicao_id`),
  ADD KEY `tb_termo_tb_empresa_id_fk` (`empresa_id`),
  ADD KEY `tb_usuario` (`usuario_id`),
  ADD KEY `tb_termo_tb_estudante_id_fk` (`estudante_id`);

--
-- Índices para tabela `tb_turno`
--
ALTER TABLE `tb_turno`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `tb_usuario`
--
ALTER TABLE `tb_usuario`
  ADD PRIMARY KEY (`co_seq_usuario`),
  ADD UNIQUE KEY `id` (`id`),
  ADD UNIQUE KEY `idx_unique_email` (`email`),
  ADD KEY `co_seq_usuario` (`co_seq_usuario`),
  ADD KEY `email` (`email`),
  ADD KEY `password_reset_token` (`password_reset_token`),
  ADD KEY `google_id` (`google_id`),
  ADD KEY `idx_usuario_seq` (`co_seq_usuario`);

--
-- Índices para tabela `tb_vaga`
--
ALTER TABLE `tb_vaga`
  ADD PRIMARY KEY (`co_seq_vaga`);

--
-- Índices para tabela `tce_docs`
--
ALTER TABLE `tce_docs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `tce_docs_tb_estudante_co_seq_estudante_fk` (`estudante_id`),
  ADD KEY `tce_docs_tb_empresa_co_seq_empresa_fk` (`empresa_id`);

--
-- Índices para tabela `termos`
--
ALTER TABLE `termos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `termos_userId_fkey` (`userId`),
  ADD KEY `termos_empresaId_fkey` (`empresaId`),
  ADD KEY `termos_representanteEmpresaId_fkey` (`representanteEmpresaId`),
  ADD KEY `termos_supervisorEmpresaId_fkey` (`supervisorEmpresaId`),
  ADD KEY `termos_instituicaoId_fkey` (`instituicaoId`),
  ADD KEY `termos_representanteInstituicaoId_fkey` (`representanteInstituicaoId`),
  ADD KEY `termos_supervisorInstituicaoId_fkey` (`supervisorInstituicaoId`),
  ADD KEY `termos_estudanteId_fkey` (`estudanteId`);

--
-- Índices para tabela `termo_realizacao_estagios`
--
ALTER TABLE `termo_realizacao_estagios`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `vagas`
--
ALTER TABLE `vagas`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `codigo_vaga` (`codigo_vaga`);

--
-- AUTO_INCREMENT de tabelas despejadas
--

--
-- AUTO_INCREMENT de tabela `prorrogacao_de_contrato`
--
ALTER TABLE `prorrogacao_de_contrato`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `representante_empresas`
--
ALTER TABLE `representante_empresas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `representante_instituicaos`
--
ALTER TABLE `representante_instituicaos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `supervisor_empresas`
--
ALTER TABLE `supervisor_empresas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `supervisor_instituicaos`
--
ALTER TABLE `supervisor_instituicaos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_categoria`
--
ALTER TABLE `tb_categoria`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_confcurso`
--
ALTER TABLE `tb_confcurso`
  MODIFY `co_seq_confcurso` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_confinstituicao`
--
ALTER TABLE `tb_confinstituicao`
  MODIFY `co_seq_confinstituicao` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_cursoinfnew`
--
ALTER TABLE `tb_cursoinfnew`
  MODIFY `co_seq_cursoinfnew` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_dados_financeiros`
--
ALTER TABLE `tb_dados_financeiros`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_depoimento`
--
ALTER TABLE `tb_depoimento`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_empresa`
--
ALTER TABLE `tb_empresa`
  MODIFY `co_seq_empresa` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_escolaridade`
--
ALTER TABLE `tb_escolaridade`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_estados`
--
ALTER TABLE `tb_estados`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_estado_civil`
--
ALTER TABLE `tb_estado_civil`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_estudante`
--
ALTER TABLE `tb_estudante`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_experprofis`
--
ALTER TABLE `tb_experprofis`
  MODIFY `co_seq_experprofis` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_instituicao`
--
ALTER TABLE `tb_instituicao`
  MODIFY `co_seq_instituicao` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_noticia`
--
ALTER TABLE `tb_noticia`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_representante`
--
ALTER TABLE `tb_representante`
  MODIFY `co_seq_representante` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_semestre`
--
ALTER TABLE `tb_semestre`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_sexo`
--
ALTER TABLE `tb_sexo`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_supervisor`
--
ALTER TABLE `tb_supervisor`
  MODIFY `co_seq_supervisor` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_termo`
--
ALTER TABLE `tb_termo`
  MODIFY `co_seq_termo` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_turno`
--
ALTER TABLE `tb_turno`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_usuario`
--
ALTER TABLE `tb_usuario`
  MODIFY `co_seq_usuario` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tb_vaga`
--
ALTER TABLE `tb_vaga`
  MODIFY `co_seq_vaga` int(10) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `tce_docs`
--
ALTER TABLE `tce_docs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `termos`
--
ALTER TABLE `termos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `vagas`
--
ALTER TABLE `vagas`
  MODIFY `codigo_vaga` int(11) NOT NULL AUTO_INCREMENT;

--
-- Restrições para despejos de tabelas
--

--
-- Limitadores para a tabela `prorrogacao_de_contrato`
--
ALTER TABLE `prorrogacao_de_contrato`
  ADD CONSTRAINT `prorrogacao_de_contrato_tb_empresa_co_seq_empresa_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`co_seq_empresa`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Limitadores para a tabela `representante_empresas`
--
ALTER TABLE `representante_empresas`
  ADD CONSTRAINT `representante_empresas_ibfk_1` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`);

--
-- Limitadores para a tabela `representante_instituicaos`
--
ALTER TABLE `representante_instituicaos`
  ADD CONSTRAINT `representante_instituicaos_ibfk_1` FOREIGN KEY (`instituicao_id`) REFERENCES `tb_instituicao` (`id`);

--
-- Limitadores para a tabela `solicitar_estagiarios`
--
ALTER TABLE `solicitar_estagiarios`
  ADD CONSTRAINT `solicitar_estagiarios_tb_confcurso_co_seq_confcurso_fk` FOREIGN KEY (`curso_id`) REFERENCES `tb_confcurso` (`co_seq_confcurso`) ON DELETE CASCADE,
  ADD CONSTRAINT `solicitar_estagiarios_tb_empresa_id_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`) ON DELETE CASCADE,
  ADD CONSTRAINT `solicitar_estagiarios_tb_semestre_id_fk` FOREIGN KEY (`semestre_id`) REFERENCES `tb_semestre` (`id`) ON DELETE CASCADE;

--
-- Limitadores para a tabela `solicitar_rescisao_termos`
--
ALTER TABLE `solicitar_rescisao_termos`
  ADD CONSTRAINT `solicitar_rescisao_termos_tb_empresa_id_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`),
  ADD CONSTRAINT `solicitar_rescisao_termos_tb_estudante_id_fk` FOREIGN KEY (`estudante_id`) REFERENCES `tb_estudante` (`id`),
  ADD CONSTRAINT `solicitar_rescisao_termos_tb_termo_id_fk` FOREIGN KEY (`termo_id`) REFERENCES `tb_termo` (`id`);

--
-- Limitadores para a tabela `solicitar_termos`
--
ALTER TABLE `solicitar_termos`
  ADD CONSTRAINT `solicitar_termos_tb_empresa_id_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`);

--
-- Limitadores para a tabela `supervisor_empresas`
--
ALTER TABLE `supervisor_empresas`
  ADD CONSTRAINT `supervisor_empresas_ibfk_1` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`);

--
-- Limitadores para a tabela `supervisor_instituicaos`
--
ALTER TABLE `supervisor_instituicaos`
  ADD CONSTRAINT `supervisor_instituicaos_ibfk_1` FOREIGN KEY (`instituicao_id`) REFERENCES `tb_instituicao` (`id`);

--
-- Limitadores para a tabela `tb_dados_financeiros`
--
ALTER TABLE `tb_dados_financeiros`
  ADD CONSTRAINT `dados_financeiros_tb_empresa_co_seq_empresa_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`co_seq_empresa`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Limitadores para a tabela `tb_empresa`
--
ALTER TABLE `tb_empresa`
  ADD CONSTRAINT `fk_user_id` FOREIGN KEY (`user_id`) REFERENCES `tb_usuario` (`co_seq_usuario`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Limitadores para a tabela `tb_estudante`
--
ALTER TABLE `tb_estudante`
  ADD CONSTRAINT `tb_estudante_tb_confcurso_co_seq_confcurso_fk` FOREIGN KEY (`curso_id`) REFERENCES `tb_confcurso` (`co_seq_confcurso`),
  ADD CONSTRAINT `tb_estudante_tb_confinstituicao_co_seq_confinstituicao_fk` FOREIGN KEY (`instituicao_id`) REFERENCES `tb_confinstituicao` (`co_seq_confinstituicao`) ON UPDATE CASCADE,
  ADD CONSTRAINT `tb_estudante_tb_escolaridade_id_fk` FOREIGN KEY (`nivel_escolaridade_id`) REFERENCES `tb_escolaridade` (`id`),
  ADD CONSTRAINT `tb_estudante_tb_estado_civil_id_fk` FOREIGN KEY (`estado_civil_id`) REFERENCES `tb_estado_civil` (`id`),
  ADD CONSTRAINT `tb_estudante_tb_estados_id_fk` FOREIGN KEY (`estado_id`) REFERENCES `tb_estados` (`id`),
  ADD CONSTRAINT `tb_estudante_tb_semestre_id_fk` FOREIGN KEY (`semestre_id`) REFERENCES `tb_semestre` (`id`),
  ADD CONSTRAINT `tb_estudante_tb_turno_id_fk` FOREIGN KEY (`turno_id`) REFERENCES `tb_turno` (`id`),
  ADD CONSTRAINT `tb_estudante_tb_usuario_co_seq_usuario_fk` FOREIGN KEY (`usuario_id`) REFERENCES `tb_usuario` (`co_seq_usuario`);

--
-- Limitadores para a tabela `tb_instituicao`
--
ALTER TABLE `tb_instituicao`
  ADD CONSTRAINT `tb_instituicao_tb_usuario_co_seq_usuario_fk` FOREIGN KEY (`user_id`) REFERENCES `tb_usuario` (`co_seq_usuario`);

--
-- Limitadores para a tabela `tb_termo`
--
ALTER TABLE `tb_termo`
  ADD CONSTRAINT `tb_instituicao` FOREIGN KEY (`instituicao_id`) REFERENCES `tb_instituicao` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `tb_termo_tb_empresa_id_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `tb_termo_tb_estudante_id_fk` FOREIGN KEY (`estudante_id`) REFERENCES `tb_estudante` (`id`),
  ADD CONSTRAINT `tb_usuario` FOREIGN KEY (`usuario_id`) REFERENCES `tb_usuario` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

--
-- Limitadores para a tabela `tce_docs`
--
ALTER TABLE `tce_docs`
  ADD CONSTRAINT `tce_docs_tb_empresa_co_seq_empresa_fk` FOREIGN KEY (`empresa_id`) REFERENCES `tb_empresa` (`co_seq_empresa`) ON DELETE CASCADE,
  ADD CONSTRAINT `tce_docs_tb_estudante_co_seq_estudante_fk` FOREIGN KEY (`estudante_id`) REFERENCES `tb_estudante` (`id`) ON DELETE CASCADE;

--
-- Limitadores para a tabela `termos`
--
ALTER TABLE `termos`
  ADD CONSTRAINT `Termos_empresaId_fkey` FOREIGN KEY (`empresaId`) REFERENCES `tb_empresa` (`co_seq_empresa`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_estudanteId_fkey` FOREIGN KEY (`estudanteId`) REFERENCES `tb_estudante` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_instituicaoId_fkey` FOREIGN KEY (`instituicaoId`) REFERENCES `tb_instituicao` (`co_seq_instituicao`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_representanteEmpresaId_fkey` FOREIGN KEY (`representanteEmpresaId`) REFERENCES `representante_empresas` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_representanteInstituicaoId_fkey` FOREIGN KEY (`representanteInstituicaoId`) REFERENCES `representante_instituicaos` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_supervisorEmpresaId_fkey` FOREIGN KEY (`supervisorEmpresaId`) REFERENCES `supervisor_empresas` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_supervisorInstituicaoId_fkey` FOREIGN KEY (`supervisorInstituicaoId`) REFERENCES `supervisor_instituicaos` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  ADD CONSTRAINT `termos_userId_fkey` FOREIGN KEY (`userId`) REFERENCES `tb_usuario` (`co_seq_usuario`) ON DELETE CASCADE ON UPDATE NO ACTION;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
