"""
Utilitaires de transformation génériques.

Fournit des fonctions de transformation réutilisables, pilotées par la configuration,
qui fonctionnent pour tous les domaines et processeurs.
"""

from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, lit, coalesce, expr as sql_expr # type: ignore
from typing import Dict, Any, List, Optional
import re
from utils.loaders import get_default_loader


def apply_conditional_transform(
    df: DataFrame,
    target_col: str,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Applique une transformation conditionnelle utilisant la logique when/otherwise.

    Construit des chaînes when().when().otherwise() à partir de dictionnaires de configuration.

    Paramètres :
        df : DataFrame en entrée
        target_col : Colonne à créer
        config : Configuration de condition avec les clés 'conditions' et 'default'

    Retourne :
        DataFrame avec la nouvelle colonne

    Exemple :
        >>> config = {
        ...     'conditions': [
        ...         {'col': 'cdpolgp1', 'op': '==', 'value': '1', 'result': 1}
        ...     ],
        ...     'default': 0
        ... }
        >>> df = apply_conditional_transform(df, 'top_coass', config)
    """
    conditions = config.get('conditions', [])
    default = config.get('default')

    expr = None
    for cond in conditions:
        cond_expr = build_condition(df, cond)

        if 'result' in cond:
            result_expr = lit(cond['result'])

        elif 'result_value' in cond:
            result_expr = lit(cond['result_value'])

        elif 'result_col' in cond:
            result_col = cond['result_col'].lower()
            result_expr = col(result_col) if result_col in df.columns else lit(None)

        elif 'result_expr' in cond:
            result_expr_str = cond['result_expr']

            # Remplacement robuste des noms de colonnes par col("...")
            for c in sorted(df.columns, key=len, reverse=True):
                result_expr_str = re.sub(
                    rf'\b{re.escape(c)}\b',
                    f'col("{c}")',
                    result_expr_str,
                    flags=re.IGNORECASE
                )

            # Eval contrôlé
            safe_globals = {"__builtins__": {}}
            safe_locals = {"col": col, "lit": lit, "when": when, "coalesce": coalesce}
            result_expr = eval(result_expr_str, safe_globals, safe_locals)

        else:
            result_expr = lit(None)

        if expr is None:
            expr = when(cond_expr, result_expr)
        else:
            expr = expr.when(cond_expr, result_expr)

    expr = expr.otherwise(lit(default)) if default is not None else expr.otherwise(lit(None))

    return df.withColumn(target_col.lower(), expr)


def build_condition(df: DataFrame, cond: Dict[str, Any]):
    """
    Construit une condition PySpark à partir d'un dictionnaire de configuration.

    Supporte :
      - Opérateurs de comparaison : ==, !=, >, <, >=, <=
      - Appartenance : in, not_in
      - Vérification de nullité : is_null, is_not_null
      - Conditions ET multi-colonnes

    Paramètres :
        df : DataFrame (pour vérifier l'existence des colonnes)
        cond : Configuration de la condition

    Retourne :
        Condition de colonne PySpark

    Exemple :
        >>> cond = {'col': 'price', 'op': '>', 'value': 100}
        >>> condition = build_condition(df, cond)
    """
    col_name = cond['col'].lower()
    op = cond.get('op', '==')
    value = cond.get('value')

    # Condition de base
    base_cond = None
    if op == '==':
        base_cond = col(col_name) == value
    elif op == '!=':
        base_cond = col(col_name) != value
    elif op == 'is_not_null':
        base_cond = col(col_name).isNotNull()
    elif op == 'is_null':
        base_cond = col(col_name).isNull()
    elif op == 'in':
        base_cond = col(col_name).isin(value)
    elif op == 'not_in':
        base_cond = ~col(col_name).isin(value)
    elif op == '>':
        base_cond = col(col_name) > value
    elif op == '<':
        base_cond = col(col_name) < value
    elif op == '>=':
        base_cond = col(col_name) >= value
    elif op == '<=':
        base_cond = col(col_name) <= value
    else:
        base_cond = lit(True)

    # Condition ET (multi-colonnes)
    if 'and_col' in cond:
        and_col = cond['and_col'].lower()
        if 'and_in' in cond:
            and_cond = col(and_col).isin(cond['and_in'])
            base_cond = base_cond & and_cond
        elif 'and_not_in' in cond:
            and_cond = ~col(and_col).isin(cond['and_not_in'])
            base_cond = base_cond & and_cond
        elif 'and_value' in cond:
            and_cond = col(and_col) == cond['and_value']
            base_cond = base_cond & and_cond

    return base_cond


def _build_expression_from_string(
    expr_str: str,
    columns: list,
    context: dict
) -> Any:
    """
    Construit une expression PySpark (Column) à partir d'une chaîne de configuration.

    Objectif :
    - Permettre d'écrire des conditions "lisibles" dans les JSON (legacy),
      puis de les transformer en expressions PySpark utilisables dans withColumn/when/filter.

    IMPORTANT :
    - On NE DOIT PAS faire expr_str.lower() car cela casse les littéraux :
      ex: '4A6160' deviendrait '4a6160' => mauvais matching.
    - On normalise seulement les références aux colonnes.

    Fonctionnalités supportées :
    - Remplacement des colonnes : noint -> col("noint")
    - Remplacement de variables de contexte : VALID_CATS -> ['A','B']
    - Null checks : "colX is null" / "colX is not null"
    - Opérateurs logiques : and / or / not
    - contains("xxx")

    Paramètres :
        expr_str : expression en texte (issue du JSON)
        columns : liste des colonnes existantes du DataFrame
        context : variables/constantes utilisables dans les expressions

    Retour :
        Une expression PySpark de type Column
    """
    # 0) Base de travail : on garde la chaîne telle quelle (sans lower-case global)
    expr_work = expr_str

    # 1) Substitution des variables de contexte (si présent)
    # Exemple : "category in VALID_CATS" avec context={"VALID_CATS": ["A","B"]}
    if context:
        for key, value in context.items():
            expr_work = re.sub(
                re.escape(key),
                repr(value),
                expr_work,
                flags=re.IGNORECASE
            )

    # 2) Substitution des noms de colonnes en col("...")
    # On trie par longueur décroissante pour éviter qu'une colonne courte
    # remplace partiellement une colonne plus longue.
    sorted_cols = sorted(columns, key=len, reverse=True)

    for c in sorted_cols:
        # \b = frontière de mot (évite de remplacer des sous-parties)
        expr_work = re.sub(
            rf'\b{re.escape(c)}\b',
            f'col("{c}")',
            expr_work,
            flags=re.IGNORECASE
        )

    # 3) Gestion des tests de nullité AVANT la conversion and/or/not
    # Sinon "is not null" peut être cassé si on remplace "not" trop tôt.
    expr_work = re.sub(
        r'(col\(["\'][^"\']+["\']\))\s+is\s+not\s+null',
        r'\1.isNotNull()',
        expr_work,
        flags=re.IGNORECASE
    )
    expr_work = re.sub(
        r'(col\(["\'][^"\']+["\']\))\s+is\s+null',
        r'\1.isNull()',
        expr_work,
        flags=re.IGNORECASE
    )

    # 4) Conversion des opérateurs logiques texte vers opérateurs PySpark
    # (en respectant les frontières de mots)
    expr_work = re.sub(r'\band\b', '&', expr_work, flags=re.IGNORECASE)
    expr_work = re.sub(r'\bor\b', '|', expr_work, flags=re.IGNORECASE)
    expr_work = re.sub(r'\bnot\b', '~', expr_work, flags=re.IGNORECASE)

    # 5) Normalisation de contains (optionnel, mais utile si config hétérogène)
    # Exemple: col("nmclt").contains('abc') -> col("nmclt").contains("abc")
    expr_work = re.sub(
        r'\.contains\s*\(\s*["\']([^"\']+)["\']\s*\)',
        r'.contains("\1")',
        expr_work,
        flags=re.IGNORECASE
    )

    # 6) Évaluation contrôlée
    # On limite l'espace de noms exposé à eval() pour éviter des surprises.
    # (Toujours préférable de migrer vers condition_sql à terme.)
    safe_globals = {"__builtins__": {}}
    safe_locals = {
        "col": col,
        "lit": lit,
        "when": when,
        "coalesce": coalesce
    }

    try:
        return eval(expr_work, safe_globals, safe_locals)
    except Exception as e:
        raise ValueError(
            f"Échec de la construction de l'expression depuis '{expr_str}': {e}\n"
            f"Traité : {expr_work}"
        )


def apply_transformations(
    df: DataFrame,
    transformations: List[Dict[str, Any]],
    context: Dict[str, Any] = None
) -> DataFrame:
    """
    Applique une série de transformations de colonnes à partir de la configuration.

    Supporte plusieurs types de transformations :
    - constant : Valeur constante simple
    - coalesce_columns : Coalesce de plusieurs colonnes
    - coalesce_default : Coalesce avec valeur par défaut littérale
    - arithmetic : Expression arithmétique
    - conditional : Logique conditionnelle (when/otherwise)
    - flag : Drapeau simple 1/0 basé sur une condition
    - mapping : Mappage de valeurs d'une colonne à une autre
    - cleanup : Remplacement de valeurs NULL/spécifiques par une autre colonne

    Paramètres :
        df : DataFrame en entrée
        transformations : Liste des configurations de transformation
        context : Dictionnaire de contexte optionnel avec dates, constantes, etc.

    Retourne :
        DataFrame avec toutes les transformations appliquées
    """
    if context is None:
        context = {}

    for transform in transformations:
        col_name = transform['column'].lower()
        transform_type = transform['type']

        if transform_type == 'constant':
            value = transform['value']
            df = df.withColumn(col_name, lit(value))

        elif transform_type == 'coalesce_columns':
            sources = [col(c.lower()) for c in transform['sources']]
            df = df.withColumn(col_name, coalesce(*sources))

        elif transform_type == 'coalesce_default':
            source = col(transform['source'].lower())
            default = lit(transform['default'])
            df = df.withColumn(col_name, coalesce(source, default))

        elif transform_type == 'arithmetic':
            expr_str = transform['expression']
            expr = _build_expression_from_string(expr_str, df.columns, context)
            df = df.withColumn(col_name, expr)

        elif transform_type == 'conditional':
            conditions = transform['conditions']
            default = transform.get('default')
            default_expr = transform.get('default_expr')

            expr = None
            for cond in conditions:
                check_str = cond['check']
                result = cond.get('result')

                cond_expr = _build_expression_from_string(check_str, df.columns, context)

                if result is not None:
                    result_expr = lit(result)
                elif 'result_col' in cond:
                    result_expr = col(cond['result_col'].lower())
                elif 'result_expr' in cond:
                    result_expr = _build_expression_from_string(cond['result_expr'], df.columns, context)
                else:
                    result_expr = col(col_name)

                if expr is None:
                    expr = when(cond_expr, result_expr)
                else:
                    expr = expr.when(cond_expr, result_expr)

            if default is not None:
                expr = expr.otherwise(lit(default))
            elif default_expr is not None:
                default_col_expr = _build_expression_from_string(default_expr, df.columns, context)
                expr = expr.otherwise(default_col_expr)
            else:
                expr = expr.otherwise(col(col_name))

            df = df.withColumn(col_name, expr)

        elif transform_type == 'flag':
            if transform.get('condition_sql'):
                cond_expr = sql_expr(transform['condition_sql'])
            else:
                condition = transform['condition']
                cond_expr = _build_expression_from_string(condition, df.columns, context)

            df = df.withColumn(col_name, when(cond_expr, lit(1)).otherwise(lit(0)))

        elif transform_type == 'mapping':
            source_col = transform.get('source', col_name).lower()
            mapping = transform.get('mapping', {})
            default = transform.get('default', '')

            expr = None
            for key, value in mapping.items():
                if expr is None:
                    expr = when(col(source_col) == key, lit(value))
                else:
                    expr = expr.when(col(source_col) == key, lit(value))

            if expr is not None:
                expr = expr.otherwise(lit(default))
                df = df.withColumn(col_name, expr)
            else:
                df = df.withColumn(col_name, lit(default))

        elif transform_type == 'cleanup':
            source = col(transform['source'].lower())
            null_values = transform.get('null_values', [None, ' '])
            replacement_col = transform.get('replacement')

            cond = source.isNull()
            for val in null_values:
                if val is not None:
                    cond = cond | (source == val)

            if replacement_col:
                replacement = col(replacement_col.lower())
            else:
                replacement = lit(None)

            df = df.withColumn(col_name, when(cond, replacement).otherwise(source))

    return df

def apply_business_filters(df, filter_config, logger=None, business_rules=None):
    """
    Applique des filtres métier provenant de la configuration JSON.

    Fonctionnalités :
    - Préserve la sémantique de gestion des NULL (souvent incluse ou exclue selon le type)
    - NOT IN conserve les lignes NULL
    - != conserve les lignes NULL
    - IN exclut les lignes NULL
    - La précédence AND/OR/NOT est respectée via le parseur sécurisé
    - Les expressions complexes sont évaluées via une analyse d'expression JSON sécurisée

    Paramètres :
        df : DataFrame en entrée.
        filter_config : Nœud JSON de configuration sous "business_filters".
        logger : Logger optionnel (pour le débogage).
        business_rules : Dictionnaire optionnel pour l'expansion de values_ref.

    Retourne :
        DataFrame filtré.
    """

    filters = filter_config.get("filters", [])
    if not filters:
        return df

    for spec in filters:
        ftype = spec["type"]

        if ftype == "equals":
            df = df.filter(col(spec["column"]) == spec["value"])

        elif ftype == "not_equals":
            val = spec["value"]
            df = df.filter(col(spec["column"]).isNull() | (col(spec["column"]) != val))

        elif ftype == "not_in":
            vals = spec.get("values", spec.get("value", []))
            if not isinstance(vals, (list, tuple, set)):
                vals = [vals]
            df = df.filter(col(spec["column"]).isNull() | (~col(spec["column"]).isin(vals)))

        elif ftype == "in":
            vals = spec["values"]
            df = df.filter(col(spec["column"]).isin(vals))

        elif ftype == "not_equals_column":
            a = spec["column"]
            b = spec["compare_column"]
            df = df.filter(col(a).isNull() | col(b).isNull() | (col(a) != col(b)))

        else:
            raise ValueError(f"Type de filtre non supporté : {ftype}")

    return df