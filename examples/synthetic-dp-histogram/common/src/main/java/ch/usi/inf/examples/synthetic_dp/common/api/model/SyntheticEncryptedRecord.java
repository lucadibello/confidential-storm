package ch.usi.inf.examples.synthetic_dp.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

public record SyntheticEncryptedRecord(EncryptedValue key, EncryptedValue count, EncryptedValue userId) {
}
