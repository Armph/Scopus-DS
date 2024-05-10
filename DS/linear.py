import pandas as pd
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def get_data():
    # nltk.download('punkt')

    df = pd.read_csv('DE/processed_data/part-00000-f7688640-2e6f-4ec8-a25f-798303556273-c000.csv')

    df = df.drop('eid', axis=1)
    #df = df.drop('author_keywords', axis=1)

    df_encoded = pd.get_dummies(df[['sub_type', 'subject_areas']])

    df = pd.concat([df, df_encoded], axis=1)

    df = df.drop(['sub_type', 'subject_areas'], axis=1)

    tfidf_vectorizer = TfidfVectorizer()
    X_tfidf = tfidf_vectorizer.fit_transform(df['author_keywords'])

    svd = TruncatedSVD(n_components=10)
    X_svd = svd.fit_transform(X_tfidf)

    for i in range(10):
        df[f'svd_{i}'] = X_svd[:, i]

    df = df.drop('author_keywords', axis=1)

    # df.to_csv('DS/processed_data.csv', index=False)

    return df

def train():
    df = pd.read_csv('DS/processed_data.csv')
    X = df.drop('refcount', axis=1)
    y = df['refcount']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    param_grid = {
        'n_estimators': [200, 300],
        'max_depth': [30]
    }

    rf = RandomForestRegressor(random_state=42)

    grid_search = GridSearchCV(rf, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1, verbose=10)

    grid_search.fit(X_train, y_train)

    print(f'Best parameters: {grid_search.best_params_}')

    rf = grid_search.best_estimator_

    joblib.dump(rf, 'DS/model.pkl')

    return

def predict():
    model = joblib.load('DS/model.pkl')
    df = get_data()

    df.drop(['refcount'], axis=1, inplace=True)

    y_pred = model.predict(df)

    df = pd.read_csv('DE/processed_data/part-00000-f7688640-2e6f-4ec8-a25f-798303556273-c000.csv')

    df['refcount'] = y_pred

    df = df.drop(['author_keywords', 'citation_count', 'eid', 'sub_type', 'length_of_abstract'], axis=1)

    df = df[['publication_year', 'subject_areas', 'refcount']]

    df = df.groupby(['publication_year', 'subject_areas']).mean().reset_index()

    df.to_csv('DS/predictions.csv', index=False)

    return

if __name__ == '__main__':
    # get_data()
    # train()
    predict()
