import logging
import pandas as pd
from data_platform_pipelines.pipelines.utils.conn.aws import AwsSession
from data_platform_pipelines.pipelines.utils.monitoring import configure_logging, FileTracking
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from databricks.sdk.runtime import *


class TableStructer:
    """
    Responsável por definir a estrutura e selecionar campos específicos das tabelas.
    """
    def __init__(self, table: str) -> None:
        """
        Inicializa a classe TableStructurer.

        Args:
            table (str): Nome da tabela a ser estruturada.
        """
        self.table = table

    def struct(self) -> StructType:
        """
        Retorna a estrutura da tabela com base no nome fornecido.

        Returns:
            StructType: Estrutura da tabela.
        """
        match self.table:
            case "cliente":
                return StructType([
                        StructField("falecido", StringType(), True),
                        StructField("enderecos", StringType(), True),
                        StructField("data_integracao", StringType(), True),
                        StructField("data_nascimento", StringType(), True),
                        StructField("tags", StringType(), True),
                        StructField("filiacao", StringType(), True),
                        StructField("nacionalidade", StringType(), True),
                        StructField("naturalidade", StringType(), True),
                        StructField("redes_sociais", StringType(), True),
                        StructField("codigo_externo", StringType(), True),
                        StructField("documentos", StringType(), True),
                        StructField("razao_social", StringType(), True),
                        StructField("id_cliente", StringType(), True),
                        StructField("estado_civil", StringType(), True),
                        StructField("telefones", StringType(), True),
                        StructField("emails", StringType(), True),
                        StructField("sigla", StringType(), True),
                        StructField("nome", StringType(), True),
                        StructField("email_principal", StringType(), True)
                    ])
            case "contrato":
                return StructType([
                        StructField("plano_ativo", StringType(), True),
                        StructField("status_status_servico", StringType(), True),
                        StructField("endereco_entrega_referencia", StringType(), True),
                        StructField("endereco_cobranca_estado_sigla", StringType(), True),
                        StructField("multa_fidelidade_periodo", StringType(), True),
                        StructField("status_data_status_faturamento", StringType(), True),
                        StructField("unidade_atendimento_cidades_atendimento", StringType(), True),
                        StructField("endereco_entrega_logradouro", StringType(), True),
                        StructField("endereco_entrega_bairro", StringType(), True),
                        StructField("dados_suspensao_inserido_blacklist", StringType(), True),
                        StructField("data_ultima_alteracao", StringType(), True),
                        StructField("endereco_entrega_cep", StringType(), True),
                        StructField("endereco_entrega_numero", StringType(), True),
                        StructField("codigo_externo", StringType(), True),
                        StructField("forma_pagamento_identificador", StringType(), True),
                        StructField("tag", StringType(), True),
                        StructField("dados_cobranca_dia_referencia_cobranca", StringType(), True),
                        StructField("unidade_atendimento_marca_associada", StringType(), True),
                        StructField("status_status_financeiro", StringType(), True),
                        StructField("data_integracao", StringType(), True),
                        StructField("forma_pagamento_nome", StringType(), True),
                        StructField("codigo_sydle", StringType(), True),
                        StructField("endereco_cobranca_referencia", StringType(), True),
                        StructField("endereco_cobranca_cidade_nome", StringType(), True),
                        StructField("status_data_status_financeiro", StringType(), True),
                        StructField("endereco_cobranca_estado_nome", StringType(), True),
                        StructField("comprador_nome", StringType(), True),
                        StructField("endereco_cobranca_cidade_codigo_ibge", StringType(), True),
                        StructField("data_venda", StringType(), True),
                        StructField("saldo_habilitacao_confianca_valor_saldo_atual", StringType(), True),
                        StructField("forma_pagamento_grupo_forma_pagamento", StringType(), True),
                        StructField("data_ativacao", StringType(), True),
                        StructField("status_status_faturamento", StringType(), True),
                        StructField("dados_suspensao_apto_suspensao", StringType(), True),
                        StructField("multa_fidelidade_data_fim", StringType(), True),
                        StructField("endereco_cobranca_cep", StringType(), True),
                        StructField("comprador_id_sydle", StringType(), True),
                        StructField("forma_pagamento_ativo", StringType(), True),
                        StructField("endereco_cobranca_complemento", StringType(), True),
                        StructField("dados_suspensao_data_aptidao", StringType(), True),
                        StructField("responsavel_venda_nome", StringType(), True),
                        StructField("endereco_cobranca_logradouro", StringType(), True),
                        StructField("endereco_entrega_estado_sigla", StringType(), True),
                        StructField("vendedor_nome", StringType(), True),
                        StructField("plano_nome", StringType(), True),
                        StructField("endereco_cobranca_numero", StringType(), True),
                        StructField("componentes", StringType(), True),
                        StructField("responsavel_venda_login", StringType(), True),
                        StructField("endereco_entrega_cidade_codigo_ibge", StringType(), True),
                        StructField("dados_cobranca_desvio_vencimento", StringType(), True),
                        StructField("status_data_status_servico", StringType(), True),
                        StructField("id_contrato", StringType(), True),
                        StructField("dados_cobranca_dia_faturamento", StringType(), True),
                        StructField("endereco_cobranca_bairro", StringType(), True),
                        StructField("unidade_atendimento_nome", StringType(), True),
                        StructField("endereco_entrega_complemento", StringType(), True),
                        StructField("endereco_entrega_pais", StringType(), True),
                        StructField("multa_fidelidade_valor_cheio_multa", StringType(), True),
                        StructField("multa_fidelidade_data_inicio", StringType(), True),
                        StructField("comprador_documento", StringType(), True),
                        StructField("vendedor_documento", StringType(), True),
                        StructField("endereco_entrega_cidade_nome", StringType(), True),
                        StructField("endereco_entrega_estado_nome", StringType(), True),
                        StructField("plano_identificador", StringType(), True),
                        StructField("saldo_habilitacao_confianca_data_ultima_habilitacao_confianca", StringType(), True),
                        StructField("dados_suspensao_regra_suspensao", StringType(), True),
                        StructField("vendedor_id_sydle", StringType(), True),
                        StructField("saldo_habilitacao_confianca_data_ultima_renovacao", StringType(), True),
                        StructField("endereco_cobranca_pais", StringType(), True),
                        StructField("dados_cobranca_dia_vencimento", StringType(), True),
                        StructField("unidade_atendimento_sigla", StringType(), True)
                ])
            case "fatura":
                return StructType([
                        StructField("data_criacao", StringType(), True),
                        StructField("id_legado", StringType(), True),
                        StructField("multa", StringType(), True),
                        StructField("tipo_documento_cliente", StringType(), True),
                        StructField("mes_referencia", StringType(), True),
                        StructField("data_geracao", StringType(), True),
                        StructField("data_atualizacao", StringType(), True),
                        StructField("forma_pagamento", StringType(), True),
                        StructField("codigo_externo", StringType(), True),
                        StructField("condicoes_pagamento", StringType(), True),
                        StructField("data_pagamento", StringType(), True),
                        StructField("classificacao", StringType(), True),
                        StructField("descricao_itens", StringType(), True),
                        StructField("id_fatura", StringType(), True),
                        StructField("codigo", StringType(), True),
                        StructField("id_cliente", StringType(), True),
                        StructField("statusfatura", StringType(), True),
                        StructField("valor_pago", StringType(), True),
                        StructField("juros", StringType(), True),
                        StructField("documento_cliente", StringType(), True),
                        StructField("data_vencimento", StringType(), True),
                        StructField("beneficiario", StringType(), True),
                        StructField("valor_total", StringType(), True),
                        StructField("cliente", StringType(), True),
                        StructField("moeda", StringType(), True),
                        StructField("data_extracao", StringType(), True),
                        StructField("valor_sem_multa_juros", StringType(), True),
                        StructField("responsavel_registro_pagamento_manual_nome", StringType(), True),
                        StructField("responsavel_registro_pagamento_manual_login", StringType(), True)
                ])
            case "nota_fiscal":
                return StructType([
                        StructField("numero_recibo", StringType(), True),
                        StructField("codigo_verificacao", StringType(), True),
                        StructField("fatura_codigo", StringType(), True),
                        StructField("emissor_documento", StringType(), True),
                        StructField("lote_nota_fiscal_numero", StringType(), True),
                        StructField("tipo", StringType(), True),
                        StructField("numero", StringType(), True),
                        StructField("tomador_endereco_sigla", StringType(), True),
                        StructField("tomador_endereco_complemento", StringType(), True),
                        StructField("tomador_endereco_referencia", StringType(), True),
                        StructField("tomador_endereco_bairro", StringType(), True),
                        StructField("tomador_endereco_cep", StringType(), True),
                        StructField("cliente_documento", StringType(), True),
                        StructField("cliente_id_sydle", StringType(), True),
                        StructField("tomador_endereco_cidade", StringType(), True),
                        StructField("cliente_nome", StringType(), True),
                        StructField("tomador_endereco_pais", StringType(), True),
                        StructField("valor_base", StringType(), True),
                        StructField("tomador_endereco_codigo_ibge", StringType(), True),
                        StructField("data_integracao", StringType(), True),
                        StructField("itens", StringType(), True),
                        StructField("data_emissao_nota_fiscal", StringType(), True),
                        StructField("lote_nota_fiscal_status", StringType(), True),
                        StructField("tomador_email", StringType(), True),
                        StructField("tomador_endereco_estado", StringType(), True),
                        StructField("data_emissao_recibo", StringType(), True),
                        StructField("fatura_id_sydle", StringType(), True),
                        StructField("entidade", StringType(), True),
                        StructField("emissor_nome", StringType(), True),
                        StructField("valor_total_impostos_retidos", StringType(), True),
                        StructField("tomador_id_sydle", StringType(), True),
                        StructField("lote_nota_fiscal_id_sydle", StringType(), True),
                        StructField("moeda", StringType(), True),
                        StructField("id_nota_fiscal", StringType(), True),
                        StructField("tomador_documento", StringType(), True),
                        StructField("serie", StringType(), True),
                        StructField("valor_total_impostos", StringType(), True),
                        StructField("tomador_endereco_numero", StringType(), True),
                        StructField("tomador_nome", StringType(), True),
                        StructField("tomador_inscricao_estadual", StringType(), True),
                        StructField("tomador_endereco_logradouro", StringType(), True),
                        StructField("emissor_id_sydle", StringType(), True),
                        StructField("status", StringType(), True),
                ])
            case "remessa":
                return StructType([
                        StructField("data_previsao_repasse", StringType(), True),
                        StructField("convenio_emissor_id_sydle", StringType(), True),
                        StructField("data_contestacao", StringType(), True),
                        StructField("data_repasse", StringType(), True),
                        StructField("pagador_nome", StringType(), True),
                        StructField("beneficiario_nome", StringType(), True),
                        StructField("beneficiario_id_sydle", StringType(), True),
                        StructField("multa", StringType(), True),
                        StructField("beneficiario_documento", StringType(), True),
                        StructField("condicao_de_pagamento_nome", StringType(), True),
                        StructField("id_remessa", StringType(), True),
                        StructField("convenio_nome", StringType(), True),
                        StructField("convenio_identificador", StringType(), True),
                        StructField("valor_taxas", StringType(), True),
                        StructField("parcelas", StringType(), True),
                        StructField("cliente_documento", StringType(), True),
                        StructField("cliente_id_sydle", StringType(), True),
                        StructField("grupo_de_forma_de_pagamento", StringType(), True),
                        StructField("detalhes_retorno", StringType(), True),
                        StructField("status_remessa", StringType(), True),
                        StructField("cliente_nome", StringType(), True),
                        StructField("condicao_de_pagamento_parcela", StringType(), True),
                        StructField("valor_estornado", StringType(), True),
                        StructField("data_status", StringType(), True),
                        StructField("convenio_ativo", StringType(), True),
                        StructField("condicao_de_pagamento_identificador", StringType(), True),
                        StructField("convenio_emissor_documento", StringType(), True),
                        StructField("data_integracao", StringType(), True),
                        StructField("pagador_documento", StringType(), True),
                        StructField("valor_pago", StringType(), True),
                        StructField("valor_liquido_repasse", StringType(), True),
                        StructField("pagador_id_sydle", StringType(), True),
                        StructField("juros", StringType(), True),
                        StructField("data_vencimento", StringType(), True),
                        StructField("codigo_remessa", StringType(), True),
                        StructField("retorno", StringType(), True),
                        StructField("faturas", StringType(), True),
                        StructField("convenio_emissor_nome", StringType(), True),
                        StructField("valor_remessa", StringType(), True),
                ])
            case "venda_qualify_vendedor":
                return StructType([
                        StructField("responsavel_analise_financeira_interna_id_sydle_one", StringType(), True),
                        StructField("justificativa_aprovacao_id_sydle_one", StringType(), True),
                        StructField("telefone_utilizado_na_venda", StringType(), True),
                        StructField("cnpj", StringType(), True),
                        StructField("duracao_total", StringType(), True),
                        StructField("resultado_analise_financeira_valor_maximo_restricoes_spc_soma", StringType(), True),
                        StructField("responsavel_analise_financeira_externa_nome", StringType(), True),
                        StructField("data_atendimento", StringType(), True),
                        StructField("analise_financeira", StringType(), True),
                        StructField("cpf", StringType(), True),
                        StructField("status_aprovacao", StringType(), True),
                        StructField("resultado_analise_financeira_classificacao_minima_serasa", StringType(), True),
                        StructField("marca_associada", StringType(), True),
                        StructField("resultado_analise_financeira_quantidade_maxima_restricoes_spc", StringType(), True),
                        StructField("data_integracao", StringType(), True),
                        StructField("parceiro", StringType(), True),
                        StructField("data_venda", StringType(), True),
                        StructField("status_venda", StringType(), True),
                        StructField("id_prospecto", StringType(), True),
                        StructField("id_sydle_one", StringType(), True),
                        StructField("data_conclusao", StringType(), True),
                        StructField("endereco_instalacao_estado", StringType(), True),
                        StructField("oferta_selecionada", StringType(), True),
                        StructField("data_ativacao", StringType(), True),
                        StructField("responsavel_pela_venda_id_sydle_one", StringType(), True),
                        StructField("unidade_sigla", StringType(), True),
                        StructField("air_codigo_contrato", StringType(), True),
                        StructField("nome_aprovacao", StringType(), True),
                        StructField("status_processo", StringType(), True),
                        StructField("condominio_id_sydle_one", StringType(), True),
                        StructField("resultado_analise_financeira_aprovado_serasa", StringType(), True),
                        StructField("data_criacao", StringType(), True),
                        StructField("prospecto", StringType(), True),
                        StructField("regional_id_sydle_one", StringType(), True),
                        StructField("nome_aprovador_final_da_venda", StringType(), True),
                        StructField("motivo_auditoria_qualify", StringType(), True),
                        StructField("endereco_instalacao_cidade", StringType(), True),
                        StructField("regional_nome", StringType(), True),
                        StructField("executor_aprovacao", StringType(), True),
                        StructField("endereco_instalacao_bairro", StringType(), True),
                        StructField("endereco_instalacao_logradouro", StringType(), True),
                        StructField("unidade_nome", StringType(), True),
                        StructField("contrato_codigo", StringType(), True),
                        StructField("justificativa_aprovacao", StringType(), True),
                        StructField("venda_refeita", StringType(), True),
                        StructField("duracao_atendimento", StringType(), True),
                        StructField("responsavel_pela_venda_nome", StringType(), True),
                        StructField("valor_total_mensalidade_com_desconto", StringType(), True),
                        StructField("responsavel_analise_financeira_interna_nome", StringType(), True),
                        StructField("responsavel_analise_financeira_externa_id_sydle_one", StringType(), True),
                        StructField("canal_venda", StringType(), True),
                        StructField("condominio_nome", StringType(), True),
                        StructField("aprovado_pela_auditoria", StringType(), True),
                        StructField("endereco_instalacao_numero", StringType(), True),
                        StructField("responsavel_analise_tecnica_nome", StringType(), True),
                        StructField("responsavel_analise_tecnica_id_sydle_one", StringType(), True)
                ])
            case "negociacao":
                return StructType([
                    StructField("data_negociacao", StringType(), True),
                    StructField("faturas_negociadas", StringType(), True),
                    StructField("origem", StringType(), True),
                    StructField("id_registro", StringType(), True),
                    StructField("id_plano_de_negociacao", StringType(), True),
                    StructField("data_alteracao", StringType(), True),
                    StructField("valor_negociado", StringType(), True),
                    StructField("tipo_de_acordo", StringType(), True),
                    StructField("parcelas", StringType(), True),
                    StructField("pedidos_negociados", StringType(), True),
                    StructField("motivo_desconto", StringType(), True),
                    StructField("regenociacao", StringType(), True),
                    StructField("quebra_de_acordo", StringType(), True),
                    StructField("valor_divida", StringType(), True),
                    StructField("responsavel_negociacao", StringType(), True),
                    StructField("codigo_externo_contrato", StringType(), True),
                    StructField("faturas_geradas", StringType(), True),
                    StructField("pedidos_gerados", StringType(), True)
                ])
            case "usuario":
                return StructType([
                    StructField("data_integracao", StringType(), True),
                    StructField("data_criacao", StringType(), True),
                    StructField("ativo", StringType(), True),
                    StructField("data_alteracao", StringType(), True),
                    StructField("id_usuario", StringType(), True),
                    StructField("nome", StringType(), True),
                    StructField("login", StringType(), True),
                    StructField("email", StringType(), True)
                ])
            case _:
                logging.warn(f"O struct da tabela {self.table} não existe.")

    def select(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Seleciona os campos específicos da tabela a partir de um DataFrame.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados.

        Returns:
            pd.DataFrame: DataFrame com os campos selecionados.
        """
        match self.table:
            case "cliente":
                return df.select(
                        "Body.falecido",
                        "Body.enderecos",
                        "Body.data_integracao",
                        "Body.data_nascimento",
                        "Body.tags",
                        "Body.filiacao",
                        "Body.nacionalidade",
                        "Body.naturalidade",
                        "Body.redes_sociais",
                        "Body.codigo_externo",
                        "Body.documentos",
                        "Body.razao_social",
                        "Body.id_cliente",
                        "Body.estado_civil",
                        "Body.telefones",
                        "Body.emails",
                        "Body.sigla",
                        "Body.nome",
                        "Body.email_principal",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                    )
            case "contrato":
                return df.select(
                        "Body.plano_ativo",
                        "Body.status_status_servico",
                        "Body.endereco_entrega_referencia",
                        "Body.endereco_cobranca_estado_sigla",
                        "Body.multa_fidelidade_periodo",
                        "Body.status_data_status_faturamento",
                        "Body.unidade_atendimento_cidades_atendimento",
                        "Body.endereco_entrega_logradouro",
                        "Body.endereco_entrega_bairro",
                        "Body.dados_suspensao_inserido_blacklist",
                        "Body.data_ultima_alteracao",
                        "Body.endereco_entrega_cep",
                        "Body.endereco_entrega_numero",
                        "Body.codigo_externo",
                        "Body.forma_pagamento_identificador",
                        "Body.tag",
                        "Body.dados_cobranca_dia_referencia_cobranca",
                        "Body.unidade_atendimento_marca_associada",
                        "Body.status_status_financeiro",
                        "Body.data_integracao",
                        "Body.forma_pagamento_nome",
                        "Body.codigo_sydle",
                        "Body.endereco_cobranca_referencia",
                        "Body.endereco_cobranca_cidade_nome",
                        "Body.status_data_status_financeiro",
                        "Body.endereco_cobranca_estado_nome",
                        "Body.comprador_nome",
                        "Body.endereco_cobranca_cidade_codigo_ibge",
                        "Body.data_venda",
                        "Body.saldo_habilitacao_confianca_valor_saldo_atual",
                        "Body.forma_pagamento_grupo_forma_pagamento",
                        "Body.data_ativacao",
                        "Body.status_status_faturamento",
                        "Body.dados_suspensao_apto_suspensao",
                        "Body.multa_fidelidade_data_fim",
                        "Body.endereco_cobranca_cep",
                        "Body.comprador_id_sydle",
                        "Body.forma_pagamento_ativo",
                        "Body.endereco_cobranca_complemento",
                        "Body.dados_suspensao_data_aptidao",
                        "Body.responsavel_venda_nome",
                        "Body.endereco_cobranca_logradouro",
                        "Body.endereco_entrega_estado_sigla",
                        "Body.vendedor_nome",
                        "Body.plano_nome",
                        "Body.endereco_cobranca_numero",
                        "Body.componentes",
                        "Body.responsavel_venda_login",
                        "Body.endereco_entrega_cidade_codigo_ibge",
                        "Body.dados_cobranca_desvio_vencimento",
                        "Body.status_data_status_servico",
                        "Body.id_contrato",
                        "Body.dados_cobranca_dia_faturamento",
                        "Body.endereco_cobranca_bairro",
                        "Body.unidade_atendimento_nome",
                        "Body.endereco_entrega_complemento",
                        "Body.endereco_entrega_pais",
                        "Body.multa_fidelidade_valor_cheio_multa",
                        "Body.multa_fidelidade_data_inicio",
                        "Body.comprador_documento",
                        "Body.vendedor_documento",
                        "Body.endereco_entrega_cidade_nome",
                        "Body.endereco_entrega_estado_nome",
                        "Body.plano_identificador",
                        "Body.saldo_habilitacao_confianca_data_ultima_habilitacao_confianca",
                        "Body.dados_suspensao_regra_suspensao",
                        "Body.vendedor_id_sydle",
                        "Body.saldo_habilitacao_confianca_data_ultima_renovacao",
                        "Body.endereco_cobranca_pais",
                        "Body.dados_cobranca_dia_vencimento",
                        "Body.unidade_atendimento_sigla",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                )
            case "fatura":
                return df.select(
                        "Body.data_criacao",
                        "Body.id_legado",
                        "Body.multa",
                        "Body.tipo_documento_cliente",
                        "Body.mes_referencia",
                        "Body.data_geracao",
                        "Body.data_atualizacao",
                        "Body.forma_pagamento",
                        "Body.codigo_externo",
                        "Body.condicoes_pagamento",
                        "Body.data_pagamento",
                        "Body.classificacao",
                        "Body.descricao_itens",
                        "Body.id_fatura",
                        "Body.codigo",
                        "Body.id_cliente",
                        "Body.statusfatura",
                        "Body.valor_pago",
                        "Body.juros",
                        "Body.documento_cliente",
                        "Body.data_vencimento",
                        "Body.beneficiario",
                        "Body.valor_total",
                        "Body.cliente",
                        "Body.moeda",
                        "Body.data_extracao",
                        "Body.valor_sem_multa_juros",
                        "Body.responsavel_registro_pagamento_manual_nome",
                        "Body.responsavel_registro_pagamento_manual_login",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                )
            case "nota_fiscal":
                return df.select(
                        "Body.numero_recibo",
                        "Body.codigo_verificacao",
                        "Body.fatura_codigo",
                        "Body.emissor_documento",
                        "Body.lote_nota_fiscal_numero",
                        "Body.tipo",
                        "Body.numero",
                        "Body.tomador_endereco_sigla",
                        "Body.tomador_endereco_complemento",
                        "Body.tomador_endereco_referencia",
                        "Body.tomador_endereco_bairro",
                        "Body.tomador_endereco_cep",
                        "Body.cliente_documento",
                        "Body.cliente_id_sydle",
                        "Body.tomador_endereco_cidade",
                        "Body.cliente_nome",
                        "Body.tomador_endereco_pais",
                        "Body.valor_base",
                        "Body.tomador_endereco_codigo_ibge",
                        "Body.data_integracao",
                        "Body.itens",
                        "Body.data_emissao_nota_fiscal",
                        "Body.lote_nota_fiscal_status",
                        "Body.tomador_email",
                        "Body.tomador_endereco_estado",
                        "Body.data_emissao_recibo",
                        "Body.fatura_id_sydle",
                        "Body.entidade",
                        "Body.emissor_nome",
                        "Body.valor_total_impostos_retidos",
                        "Body.tomador_id_sydle",
                        "Body.lote_nota_fiscal_id_sydle",
                        "Body.moeda",
                        "Body.id_nota_fiscal",
                        "Body.tomador_documento",
                        "Body.serie",
                        "Body.valor_total_impostos",
                        "Body.tomador_endereco_numero",
                        "Body.tomador_nome",
                        "Body.tomador_inscricao_estadual",
                        "Body.tomador_endereco_logradouro",
                        "Body.emissor_id_sydle",
                        "Body.status",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                )
            case "remessa":
                return df.select(
                        "Body.data_previsao_repasse",
                        "Body.convenio_emissor_id_sydle",
                        "Body.data_contestacao",
                        "Body.data_repasse",
                        "Body.pagador_nome",
                        "Body.beneficiario_nome",
                        "Body.beneficiario_id_sydle",
                        "Body.multa",
                        "Body.beneficiario_documento",
                        "Body.condicao_de_pagamento_nome",
                        "Body.id_remessa",
                        "Body.convenio_nome",
                        "Body.convenio_identificador",
                        "Body.valor_taxas",
                        "Body.parcelas",
                        "Body.cliente_documento",
                        "Body.cliente_id_sydle",
                        "Body.grupo_de_forma_de_pagamento",
                        "Body.detalhes_retorno",
                        "Body.status_remessa",
                        "Body.cliente_nome",
                        "Body.condicao_de_pagamento_parcela",
                        "Body.valor_estornado",
                        "Body.data_status",
                        "Body.convenio_ativo",
                        "Body.condicao_de_pagamento_identificador",
                        "Body.convenio_emissor_documento",
                        "Body.data_integracao",
                        "Body.pagador_documento",
                        "Body.valor_pago",
                        "Body.valor_liquido_repasse",
                        "Body.pagador_id_sydle",
                        "Body.juros",
                        "Body.data_vencimento",
                        "Body.codigo_remessa",
                        "Body.retorno",
                        "Body.faturas",
                        "Body.convenio_emissor_nome",
                        "Body.valor_remessa",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                )
            case "venda_qualify_vendedor":
                return df.select(
                        "Body.responsavel_analise_financeira_interna_id_sydle_one",
                        "Body.justificativa_aprovacao_id_sydle_one",
                        "Body.telefone_utilizado_na_venda",
                        "Body.cnpj",
                        "Body.duracao_total",
                        "Body.resultado_analise_financeira_valor_maximo_restricoes_spc_soma",
                        "Body.responsavel_analise_financeira_externa_nome",
                        "Body.data_atendimento",
                        "Body.analise_financeira",
                        "Body.cpf",
                        "Body.status_aprovacao",
                        "Body.resultado_analise_financeira_classificacao_minima_serasa",
                        "Body.marca_associada",
                        "Body.resultado_analise_financeira_quantidade_maxima_restricoes_spc",
                        "Body.data_integracao",
                        "Body.parceiro",
                        "Body.data_venda",
                        "Body.status_venda",
                        "Body.id_prospecto",
                        "Body.id_sydle_one",
                        "Body.data_conclusao",
                        "Body.endereco_instalacao_estado",
                        "Body.oferta_selecionada",
                        "Body.data_ativacao",
                        "Body.responsavel_pela_venda_id_sydle_one",
                        "Body.unidade_sigla",
                        "Body.air_codigo_contrato",
                        "Body.nome_aprovacao",
                        "Body.status_processo",
                        "Body.condominio_id_sydle_one",
                        "Body.resultado_analise_financeira_aprovado_serasa",
                        "Body.data_criacao",
                        "Body.prospecto",
                        "Body.regional_id_sydle_one",
                        "Body.nome_aprovador_final_da_venda",
                        "Body.motivo_auditoria_qualify",
                        "Body.endereco_instalacao_cidade",
                        "Body.regional_nome",
                        "Body.executor_aprovacao",
                        "Body.endereco_instalacao_bairro",
                        "Body.endereco_instalacao_logradouro",
                        "Body.unidade_nome",
                        "Body.contrato_codigo",
                        "Body.justificativa_aprovacao",
                        "Body.venda_refeita",
                        "Body.duracao_atendimento",
                        "Body.responsavel_pela_venda_nome",
                        "Body.valor_total_mensalidade_com_desconto",
                        "Body.responsavel_analise_financeira_interna_nome",
                        "Body.responsavel_analise_financeira_externa_id_sydle_one",
                        "Body.canal_venda",
                        "Body.condominio_nome",
                        "Body.aprovado_pela_auditoria",
                        "Body.endereco_instalacao_numero",
                        "Body.responsavel_analise_tecnica_nome",
                        "Body.responsavel_analise_tecnica_id_sydle_one",
                        "HTTPHeaders",
                        "RetryAttempts",
                        "MessageId",
                        "RequestId"
                )
            case "negociacao":
                return df.select(
                    "Body.data_negociacao",
                    "Body.faturas_negociadas",
                    "Body.origem",
                    "Body.id_registro",
                    "Body.id_plano_de_negociacao",
                    "Body.data_alteracao",
                    "Body.valor_negociado",
                    "Body.tipo_de_acordo",
                    "Body.parcelas",
                    "Body.pedidos_negociados",
                    "Body.motivo_desconto",
                    "Body.regenociacao",
                    "Body.quebra_de_acordo",
                    "Body.valor_divida",
                    "Body.responsavel_negociacao",
                    "Body.codigo_externo_contrato",
                    "Body.faturas_geradas",
                    "Body.pedidos_gerados",
                    "HTTPHeaders",
                    "RetryAttempts",
                    "MessageId",
                    "RequestId"
                )
            case "usuario":
                return df.select(
                    "Body.data_integracao",
                    "Body.data_criacao",
                    "Body.ativo",
                    "Body.data_alteracao",
                    "Body.id_usuario",
                    "Body.nome",
                    "Body.login",
                    "Body.email",
                    "HTTPHeaders",
                    "RetryAttempts",
                    "MessageId",
                    "RequestId"
                )
            case _:
                logging.warn(f"A query que gera a tabela {self.table} não existe.")


class SqsProcesser:
    def __init__(self, tables, input_path, outputs):
        """
        Inicializa a classe DataPipeline.

        Args:
            tables (list): Lista de tabelas a serem processadas.
            input_path (str): Caminho de entrada no S3.
            outputs (dict): Dicionário com informações de saída (Unity Catalog
            e S3).
        """
        self.tables = tables
        self.input_path = input_path
        self.outputs = outputs
        self.aws_manager = AwsSession()
        self.s3 = self.aws_manager.get_client_to(service="s3")

    def process_tables(self):
        """
        Processa cada tabela na lista de tabelas.
        """
        configure_logging()

        for table in self.tables:
            logging.info(f"Iniciando o job da tabela {table}")
            
            bucket, prefix, catalog_schema_table, s3_out_path = self._configure_paths(table)
            
            logs_audit = FileTracking(
                table=table,
                env="dev" if "-dev-" in self.input_path else "prd"
            )

            folders_to_read = []
            collected = logs_audit.get_paths_to_read(max_no_tries=5)
            
            # Finds each path that wasn't read before by using groupby path and max on boolean column
            agg = pd.DataFrame(collected.toPandas().groupby(['path'])['readed'].max()).reset_index()
            agg.dropna(subset='path', inplace=True)
            agg.drop(agg.loc[agg.path == 'null'].index, inplace=True)
            agg.drop_duplicates(subset='path', inplace=True)
            folders_to_read = agg.loc[agg.readed == False].path.to_list()
            
            if len(folders_to_read) == 0:
                logging.info(f"Não há pastas não processadas para a tabela: {table}")
                continue
            
            logging.info(f'Pastas a processar: {folders_to_read}')
            for s3_path_to_read in folders_to_read:
                logging.info(f'Lendo a pasta: {s3_path_to_read}')
                logs_audit.path = s3_path_to_read
                
                structer = TableStructer(table)
                df = spark.read.parquet(s3_path_to_read)
                json_schema = structer.struct()
                struct_df = df.withColumn("Body", from_json(col("Body"), json_schema))
                result_df = structer.select(struct_df)

                self._create_table_if_not_exists(catalog_schema_table, s3_out_path)
                self._append_to_table(catalog_schema_table, result_df, table)

                logs_audit.append_readed_path(max_no_tries = 15)

    def _configure_paths(self, table):
        """
        Configura os caminhos de bucket, prefixo, tabela de catálogo e caminho de saída no S3.

        Args:
            table (str): Nome da tabela.

        Returns:
            tuple: bucket, prefix, catalog_schema_table, s3_out_path.
        """
        bucket = self.input_path.split("/")[2]
        prefix = "/".join(self.input_path.split("/")[3:]).format(
            origin="sqs",
            schema="sydle",
            table=table
        )
        catalog_schema_table = ".".join(
            self.outputs["unity_catalog"].values()
        ).format(schema="sydle", table=table)
        s3_out_path = self.outputs["s3"].format(
            origin="sqs",
            schema="sydle",
            table=table
        )

        return bucket, prefix, catalog_schema_table, s3_out_path

    def _get_latest_object(self, bucket, prefix):
        """
        Obtém o objeto mais recente em um bucket/prefixo do S3.

        Args:
            bucket (str): Nome do bucket.
            prefix (str): Prefixo no bucket.

        Returns:
            dict: Objeto S3 mais recente.
        """
        response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        latest_obj = None

        logging.info(f"Listando objetos do bucket: {bucket}")
        while True:
            if "Contents" in response:
                for obj in response["Contents"]:
                    if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                        latest_obj = obj

            if "NextContinuationToken" in response:
                response = self.s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=response["NextContinuationToken"]
                )
            elif "ContinuationToken" in response:
                response = self.s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=response["ContinuationToken"]
                )
            else:
                break

        logging.debug(f"Latest Object: {latest_obj}")
        return latest_obj

    def _create_table_if_not_exists(self, catalog_schema_table, s3_out_path):
        """
        Cria uma tabela no Unity Catalog se ela não existir.

        Args:
            catalog_schema_table (str): Nome da tabela no Unity Catalog.
            s3_out_path (str): Caminho de saída no S3.
        """
        logging.info(f"Tentando criar a tabela: {catalog_schema_table}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_schema_table}
            LOCATION '{s3_out_path}';
            """
        )

    def _append_to_table(self, catalog_schema_table, result_df, table):
        """
        Realiza o append dos dados processados na tabela do Unity Catalog.

        Args:
            catalog_schema_table (str): Nome da tabela no Unity Catalog.
            result_df (DataFrame): DataFrame com os dados processados.
            table (str): Nome da tabela.
        """
        logging.info(f"Iniciando appending na {catalog_schema_table}")
        (
            result_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(catalog_schema_table)
        )
        logging.info(f'{table} sofreu append de {result_df.count()} linhas\n---')
