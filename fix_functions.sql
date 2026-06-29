CREATE OR REPLACE FUNCTION mask_email(email VARCHAR) RETURNS VARCHAR AS 
BEGIN
    IF email IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN 'SHA256:' || encode(digest(email::bytea, 'sha256'), 'hex');
END;
 LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION mask_phone(phone VARCHAR) RETURNS VARCHAR AS 
BEGIN
    IF phone IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN 'SHA256:' || encode(digest(phone::bytea, 'sha256'), 'hex');
END;
 LANGUAGE plpgsql IMMUTABLE;
