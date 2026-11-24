
{% macro create_latest_version_view() %}

    -- This hook runs only when the current model is versioned
    -- and the current version is the latest version.
    --
    -- For all other models and older versions, it does nothing.

    {% if model.get('version') and model.get('version') == model.get('latest_version') %}

        -- Create a relation using the original model name
        -- without the version suffix.
        {% set new_relation = this.incorporate(path={"identifier": model['name']}) %}

        -- Check whether a relation with this name already exists.
        {% set existing_relation = load_relation(new_relation) %}

        -- If the existing relation is a table rather than a view,
        -- remove it so that the compatibility view can be created.
        {% if existing_relation and not existing_relation.is_view %}
            {{ drop_relation_if_exists(existing_relation) }}
        {% endif %}

        -- Create or replace the original model name as a view
        -- pointing to the latest version.
        {% set create_view_sql %}

            CREATE OR REPLACE VIEW {{ new_relation }}
            AS
            SELECT *
            FROM {{ this }}

        {% endset %}

        -- Log the view creation in the dbt terminal.
        {% do log("Creating view " ~ new_relation ~ " pointing to " ~ this, info = true) if execute %}

        {{ return(create_view_sql) }}

    {% else %}

        -- No operation for non-latest models.
        SELECT 1 AS id

    {% endif %}

{% endmacro %}
