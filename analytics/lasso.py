from sklearn import linear_model

def Lasso(mat_builder, vec, alpha=1.0):
    lasso = linear_model.Lasso(fit_intercept=False, alpha=alpha)
    lasso.fit(mat_builder.CSR(), vec)
    result = {}
    for i, val in enumerate(lasso.coef_):
        result[mat_builder.GetColKey(i)] = val
    return result
