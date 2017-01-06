from sklearn import linear_model

def Lasso(mat_builder, vec, remove_zeros=True, alpha=1.0):
    lasso = linear_model.Lasso(fit_intercept=False, alpha=alpha)
    lasso.fit(mat_builder.CSR(), vec)
    result = {}
    for i, val in enumerate(lasso.coef_):
        if (not remove_zeros) or val != 0:
            result[mat_builder.GetColKey(i)] = val
    return result
