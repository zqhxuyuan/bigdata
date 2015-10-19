package com.github.lbhat1.mlstorm.batch.dataobject.label;

import java.io.Serializable;

public interface Label extends Serializable {
    String toString();

    double getLabelValue();
}
